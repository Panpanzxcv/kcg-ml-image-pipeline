import os
import sys
import torch
import datasets

import torch.nn.functional as F
from diffusers.optimization import get_scheduler
from diffusers import AutoPipelineForText2Image, DDPMScheduler, UNet2DConditionModel, VQModel
from transformers import CLIPImageProcessor, CLIPVisionModelWithProjection

from PIL import Image
import numpy as np
from tqdm.auto import tqdm

base_dir = "./"
sys.path.insert(0, base_dir)
sys.path.insert(0, os.getcwd())
from kandinsky.model_paths import PRIOR_MODEL_PATH, DECODER_MODEL_PATH

import hashlib

def get_md5(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

file_path = 'input/pokemon-blip-captions/train/data-00000-of-00001.arrow'
md5_checksum = get_md5(file_path)
print(f"MD5 checksum: {md5_checksum}")

weight_dtype = torch.float32
# weight_dtype = torch.float16

device = torch.device(0)

optimizer_cls = torch.optim.AdamW

learning_rate = 1e-4

adam_beta1 = 0.9
adam_beta2 = 0.999
adam_weight_decay = 0.0
adam_epsilon = 1e-8

snr_gamma = None

max_train_steps = 10000
gradient_accumulation_steps = 1
checkpointing_steps = 100

lr_scheduler = 'constant'
lr_warmup_steps = 500

train_batch_size = 1
dataloader_num_workers = 1

image_column = 'image'
resolution = 512

local_files_only = False
pretrained_prior_model_name_or_path = PRIOR_MODEL_PATH
pretrained_decoder_model_name_or_path = DECODER_MODEL_PATH
noise_scheduler = DDPMScheduler.from_pretrained(
    pretrained_decoder_model_name_or_path, subfolder="scheduler",
    local_files_only=local_files_only
)
image_processor = CLIPImageProcessor.from_pretrained(
    pretrained_prior_model_name_or_path, subfolder="image_processor",
    local_files_only=local_files_only
)
vae = VQModel.from_pretrained(
    pretrained_decoder_model_name_or_path, subfolder="movq", torch_dtype=weight_dtype,
    local_files_only=local_files_only
).eval()
image_encoder = CLIPVisionModelWithProjection.from_pretrained(
    pretrained_prior_model_name_or_path, subfolder="image_encoder", torch_dtype=weight_dtype,
    local_files_only=local_files_only
).eval()
unet = UNet2DConditionModel.from_pretrained(
    pretrained_decoder_model_name_or_path, subfolder="unet",
    local_files_only=local_files_only
)
# Freeze vae and image_encoder
vae.requires_grad_(False)
image_encoder.requires_grad_(False)

unet.enable_gradient_checkpointing()

# Move image_encode and vae to gpu and cast to weight_dtype
image_encoder.to(device, dtype=weight_dtype)
vae.to(device, dtype=weight_dtype)
unet.to(device, dtype=weight_dtype)
optimizer = optimizer_cls(
    unet.parameters(),
    lr=learning_rate,
    betas=(adam_beta1, adam_beta2),
    weight_decay=adam_weight_decay,
    eps=adam_epsilon,
)
def center_crop(image):
    width, height = image.size
    new_size = min(width, height)
    left = (width - new_size) / 2
    top = (height - new_size) / 2
    right = (width + new_size) / 2
    bottom = (height + new_size) / 2
    return image.crop((left, top, right, bottom))

def train_transforms(img):
    img = center_crop(img)
    img = img.resize((resolution, resolution), resample=Image.BICUBIC, reducing_gap=1)
    img = np.array(img).astype(np.float32) / 127.5 - 1
    img = torch.from_numpy(np.transpose(img, [2, 0, 1]))
    return img

def preprocess_train(examples):
    images = [image.convert("RGB") for image in examples[image_column]]
    examples["pixel_values"] = [train_transforms(image) for image in images]
    examples["clip_pixel_values"] = image_processor(images, return_tensors="pt").pixel_values
    return examples
dataset = datasets.load_dataset('arrow', data_files={'train': 'input/pokemon-blip-captions/train/data-00000-of-00001.arrow'})

# Set the training transforms
train_dataset = dataset["train"].with_transform(preprocess_train)
def collate_fn(examples):
    pixel_values = torch.stack([example["pixel_values"] for example in examples])
    pixel_values = pixel_values.to(memory_format=torch.contiguous_format).float()
    clip_pixel_values = torch.stack([example["clip_pixel_values"] for example in examples])
    clip_pixel_values = clip_pixel_values.to(memory_format=torch.contiguous_format).float()
    return {"pixel_values": pixel_values, "clip_pixel_values": clip_pixel_values}

train_dataloader = torch.utils.data.DataLoader(
    train_dataset,
    shuffle=True,
    collate_fn=collate_fn,
    batch_size=train_batch_size,
    num_workers=dataloader_num_workers,
)
lr_scheduler = get_scheduler(
    lr_scheduler,
    optimizer=optimizer,
    num_warmup_steps=lr_warmup_steps * gradient_accumulation_steps,
    num_training_steps=max_train_steps * gradient_accumulation_steps,
)
progress_bar = tqdm(range(0, max_train_steps), desc="Steps")

epoch = 1
step = 0
data_iter = iter(train_dataloader)

losses = list()

# # Initialize the gradient scaler for mixed precision training
# scaler = torch.cuda.amp.GradScaler()

while step < max_train_steps:
    
    try:
        batch = next(data_iter)
    except:
        epoch += 1
        data_iter = iter(train_dataloader)
        batch = next(data_iter)
        print(f"Epoch: {epoch}, Step: {step}, Loss: {np.mean(losses)}")
        losses = list()
    
    # Set unet to trainable.
    unet.train()

    # Convert images to latent space
    with torch.no_grad():
        images = batch["pixel_values"].to(device, weight_dtype)
        clip_images = batch["clip_pixel_values"].to(device, weight_dtype)
        latents = vae.encode(images).latents
        image_embeds = image_encoder(clip_images).image_embeds
    
    # Sample noise that we'll add to the latents
    noise = torch.randn_like(latents)
    bsz = latents.shape[0]
    # Sample a random timestep for each image
    timesteps = torch.randint(0, noise_scheduler.config.num_train_timesteps, (bsz,), device=latents.device)
    timesteps = timesteps.long()

    noisy_latents = noise_scheduler.add_noise(latents, noise, timesteps)
    target = noise

    with torch.cuda.amp.autocast(True):
        
        # Predict the noise residual and compute loss
        added_cond_kwargs = {"image_embeds": image_embeds}
    
        model_pred = unet(noisy_latents, timesteps, None, added_cond_kwargs=added_cond_kwargs).sample[:, :4]
    
        if snr_gamma is None:
            loss = F.mse_loss(model_pred.float(), target.float(), reduction="mean")
        else:
            snr = compute_snr(noise_scheduler, timesteps)
            mse_loss_weights = torch.stack([snr, snr_gamma * torch.ones_like(timesteps)], dim=1).min(dim=1)[0]
            if noise_scheduler.config.prediction_type == "epsilon":
                mse_loss_weights = mse_loss_weights / snr
            elif noise_scheduler.config.prediction_type == "v_prediction":
                mse_loss_weights = mse_loss_weights / (snr + 1)
        
            loss = F.mse_loss(model_pred.float(), target.float(), reduction="none")
            loss = loss.mean(dim=list(range(1, len(loss.shape)))) * mse_loss_weights
            loss = loss.mean()
        
        loss = loss / gradient_accumulation_steps  # Adjust loss for gradient accumulation
        
    loss.backward()
    step += 1
    if step % gradient_accumulation_steps == 0:
        # Performs the optimizer step
        optimizer.step()
        # Update the learning rate
        lr_scheduler.step()
        optimizer.zero_grad()
        progress_bar.update(1)

    # scaler.scale(loss).backward()
    # step += 1
    # if step % gradient_accumulation_steps == 0:
    #     # Performs the optimizer step
    #     scaler.step(optimizer)
    #     scaler.update()
    #     # Update the learning rate
    #     lr_scheduler.step()
    #     optimizer.zero_grad()
    #     progress_bar.update(1)

    losses.append(loss.detach().cpu().numpy())
    
    # Save model periodically or based on conditions
    if step % checkpointing_steps == 0:
        print(f"Epoch: {epoch}, Step: {step}, Loss: {np.mean(losses)}")
        losses = list()
        torch.save(unet.state_dict(), f"unet.pth")
        # torch.save(unet.state_dict(), f"unet_epoch_{epoch}_step_{step}.pth")