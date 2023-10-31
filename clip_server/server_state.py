import sys

base_directory = "./"
sys.path.insert(0, base_directory)

from utility.clip.clip import ClipModel


class Phrase:
    def __init__(self, id, phrase):
        self.id = id
        self.phrase = phrase

class ClipVector:
    def __init__(self, phrase, clip_vector):
        self.phrase = phrase
        self.clip_vector = clip_vector

class ClipServer:
    def __init__(self):
        self.id_counter = 0
        self.phrase_dictionary = {}
        self.clip_vector_dictionary = {}
        self.clip_model = ClipModel()

    def load_clip_model(self):
        self.clip_model.load_clip()
        self.clip_model.load_tokenizer()

    def generate_id(self):
        new_id = self.id_counter
        self.id_counter = self.id_counter + 1

        return new_id

    def add_phrase(self, phrase):
        new_id = self.generate_id()
        clip_vector = self.compute_clip_vector(phrase)

        new_phrase = Phrase(new_id, phrase)
        new_clip_vector = ClipVector(phrase, clip_vector)

        self.phrase_dictionary[new_id] = new_phrase
        self.clip_vector_dictionary[phrase] = new_clip_vector

        return new_phrase

    def get_clip_vector(self, phrase):
        if phrase in self.clip_vector_dictionary:
            return self.clip_vector_dictionary[phrase]

        return []

    def get_phrase_list(self, offset, limit):
        result = []
        count = 0
        for key, value in self.phrase_dictionary.items():
            if count >= offset:
                if count < offset + limit:
                    result.append(value)
                else:
                    break
            count += 1
        return result

    def compute_clip_vector(self, text):
        clip_vector_gpu = self.clip_model.get_text_features(text)
        clip_vector_cpu = clip_vector_gpu.cpu()

        del clip_vector_gpu

        clip_vector = clip_vector_cpu.tolist()
        return clip_vector





