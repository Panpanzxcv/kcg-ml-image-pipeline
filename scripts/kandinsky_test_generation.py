import io
import sys
import os
import uuid
import torch


base_directory = os.getcwd()
sys.path.insert(0, base_directory)

from utility.http import generation_request
from utility.http import request
from utility.minio.cmd import connect_to_minio_client, upload_data
from worker.generation_task.generation_task import GenerationTask
from kandinsky_worker.dataloaders.image_embedding import ImageEmbedding

def read_msgpack_file(file_path):
    """Reads a msgpack file and returns its bytes."""
    with open(file_path, "rb") as f:
        return f.read()

def generate_img2img_generation_jobs_with_kandinsky(image_embedding,
                                                    negative_image_embedding,
                                                    dataset_name,
                                                    prompt_generation_policy,
                                                    self_training= False,
                                                    init_img_path="./test/test_inpainting/white_512x512.jpg"):

    # get sequential ids
    sequential_ids = request.http_get_sequential_id(dataset_name, 1)

    count = 0
    # generate UUID
    task_uuid = str(uuid.uuid4())
    task_type = "img2img_generation_kandinsky"
    model_name = "kandinsky_2_2"
    model_file_name = "kandinsky-2-2-decoder"
    model_file_path = "input/model/kandinsky/kandinsky-2-2-decoder"
    task_input_dict = {
        "strength": 0.75,
        "seed": "",
        "dataset": dataset_name,
        "file_path": sequential_ids[count]+".jpg",
        "init_img": init_img_path,
        "num_images": 1,
        "image_width": 512,
        "image_height": 512,
        "decoder_steps": 100,
        "decoder_guidance_scale": 12,
        "self_training": self_training
    }

    prompt_generation_data={
        "prompt_generation_policy": prompt_generation_policy
    }

    image_embedding= image_embedding.detach().cpu().numpy().tolist()[0]
    negative_image_embedding= image_embedding.detach().cpu().numpy().tolist()[0] if negative_image_embedding is not None else None

    # create the job
    generation_task = GenerationTask(uuid=task_uuid,
                                     task_type=task_type,
                                     model_name=model_name,
                                     model_file_name=model_file_name,
                                     model_file_path=model_file_path,
                                     task_input_dict=task_input_dict,
                                     prompt_generation_data=prompt_generation_data)
    generation_task_json = generation_task.to_dict()

    # add job
    response = generation_request.http_add_kandinsky_job(job=generation_task_json,
                                                         positive_embedding=image_embedding,
                                                         negative_embedding=negative_image_embedding)

    return response


if __name__ == "__main__":
    image_embedding_list = [-8.106171137869465, 2.4239269647453257, -9.509251938616277, -4.274501974368199, 2.9273579497308932, 4.281494071381887, -2.236305694878019, 2.2596126849236473, -10.366949172295392, -3.896928735629024, -4.050754869930169, -9.350764406306007, 2.207171957320984, 1.8342601165909338, 5.052955441892179, 4.941081889673164, -2.335360402571939, 1.4357105868106927, 3.9738418027795963, 0.16154657475376, -3.6731816311909937, 1.255081413957075, -5.402560292576601, -3.3049311884700687, 3.0205859099134056, -0.3650457315896506, 4.903790705600159, -5.379253302530973, -7.197198526089967, 1.0878537603796932, -2.7875160094571245, -11.103450057737243, -4.1136837430533655, -5.8081019193705306, 4.642752417089124, 0.3676677679697838, 2.8574369795940093, -0.536060771049447, 1.5067969064498588, 7.859117043385806, 4.791917153381144, 1.4508601303403512, -2.1873610157822, -1.1222315706969945, 3.6941579222320584, 1.8016303305270545, -13.014623241478748, -6.083124401908942, -0.54684025394555, -5.155506198092943, 2.153565880216039, -7.73792069514854, -5.3419621184579675, -3.1883962382419284, 0.7702960210080099, 3.6871658252183703, -7.55612617279264, -1.6897567783080394, 1.0697908430943313, 2.624367079137728, -1.459017576856321, -0.19665272850998738, 19.633808414437137, -2.58474519606016, 1.4823245669019491, 0.4626437524057184, 1.9414722708008234, -2.1628886762342905, -4.561177951929426, 1.8016303305270545, 4.586815640979617, 3.558977379967416, 6.76834990925041, 0.1229443724906884, -4.533209563874672, 1.619835808171155, 7.099309167898329, 1.3075221415597382, 4.8385311334724, -10.171170455912117, -7.388315844464117, -1.4357105868106927, -4.528548165865546, -0.4530296190118968, -2.034700230983336, 4.87116091953628, 12.324736336128158, 7.430268426546248, 2.6942880492746117, 5.290686740357586, -0.608895114942035, 0.9940431254460398, -7.551464774783515, 2.9390114447537075, -4.654405912111938, 0.18936929412072856, -2.668650360224421, 4.057746966943857, -7.351024660391112, -6.521295814766751, -4.847853929490651, -4.335100148486832, -1.7783233404814265, -6.707751735131776, 9.956746147492337, -0.3187230888739647, -4.0321092778936665, 1.2935379475323614, -8.842672023311312, 3.416804740689084, -4.6940277951895055, -5.579693416923375, 3.5543159819582906, -5.6449529890511325, -1.1869084680736126, -2.4635488478228935, 8.413823406471755, 1.4869859649110748, 1.333159830609929, -1.4007501017422508, 2.4868558378685215, -4.833869735463274, -2.456556750809205, 4.323446653464019, -4.335100148486832, -0.3245498363853717, -0.4661398009125627, 5.7475037452518976, 5.924636869598671, 1.2376011714228539, 0.3493135133088516, -5.411883088594852, 1.694418176317165, -1.238766520925135, -5.836070307425284, -7.570110366820018, 0.09526732181150499, 1.0639640955829242, -3.0205859099134056, -6.465359038657244, 14.43168823625294, 11.420425122357784, -1.694418176317165, -1.5196157509749542, -4.111353044048802, -7.4209456305279975, 9.374071396351635, 5.700889765160641, -1.7223865643719187, 3.540331787930913, -3.27230140240619, -3.0205859099134056, -1.0033659214642912, 12.697648176858207, -4.787255755372018, 2.710602942306552, -1.7387014574038586, -3.8339998625058276, 6.810302491332541, -2.1372509871840997, -0.120540839142233, -0.4180691339434546, 3.8339998625058276, -5.076262431937807, 4.384044827582652, -5.006341461800923, 2.000905095417175, -4.493587680797104, -4.703350591207757, 0.1384580877398096, -2.032369531978773, -0.2106369225373642, -0.22840850244715571, 1.8494096601205923, -5.379253302530973, -2.235140345375738, 5.458497068686109, -3.8433226585240785, 3.2862855964335664, -1.246923967441105, -4.733649678267073, 6.996758411697566, 0.10684798249042646, -7.3603474564093645, -1.5895367211118385, 0.5319820477914621, 4.0927074520122995, -1.7468589039198283, -1.2189555793863514, 3.8992594346335863, -4.516894670842732, 2.0486844250107126, 6.194997954127958, 8.0036203816687, -0.9678227616447082, -1.3459786751350247, -5.761487939279274, -2.705941544297426, -5.421205884613104, 14.058776395522889, -1.2725616564912958, 0.6694932890606681, -18.347262563918466, -2.244463141393989, -1.4636789748654466, 3.465749419784903, -0.9031458642680901, -4.726657581253385, 27.241209965330164, -1.9298187757780094, -4.917774899627536, 4.365399235546149, 0.15470014642785673, -4.339761546495958, 4.274501974368199, -0.9619960141333012, -4.0530855689347325, -5.649614387060259, -1.5196157509749542, 0.28958935131692953, 5.188135984156822, -2.109282599129346, -11.401779530321281, 3.407481944670833, 4.116014442057928, -2.8667597756122603, -1.4252224412901602, 0.8139966273435626, 11.951824495398107, -5.938621063626048, 12.753584952967714, 0.6572571192867133, -0.7196033176587685, 1.65712699224416, -0.5905408602811028, 4.796578551390269, -2.27010083044418, 8.06887995379646, 2.201345209809577, -3.5263475939035365, -1.097759231149085, -5.393237496558349, -7.295087884281605, 3.2000497332647426, 5.3885760985492235, -2.3435178490879087, -3.980833899793285, 1.0121060427314017, 5.8407317054344094, 7.649354132975152, 0.6275407069785375, 14.189295539778406, -10.814443381171452, 6.302210108337847, -4.894467909581907, -7.915053819495314, -6.525957212775877, 1.720055865367356, -8.17609210800635, 0.7726267200125726, 0.44603752199820834, -8.101509739860338, -6.474681834675495, -0.7120285458939395, -10.301689600167634, -8.087525545832962, -9.490606346579774, 1.5860406726049945, 5.7801335313157765, -4.1276679370807425, -12.091666435671875, 1.0534759500623916, -1.7025756228331352, 3.3189153824974458, -4.407351817628279, -2.8364606885529438, -7.5421419787652635, 8.800719441229182, 4.917774899627536, 6.567909794858008, -8.07820274981471, 5.146183402074691, -2.355171344110723, 3.8013700764419482, 7.733259297139414, -1.9065117857323812, 6.153045372045827, 1.3611282186646827, 6.400099466529485, 8.712152879055795, 1.8867008441935973, -19.764327558692656, -6.4280678545842385, -5.635630193032882, -1.0907671341353966, -4.570500747947676, 2.303895966010341, 0.2656996865201607, -4.931759093654913, 2.2596126849236473, 3.558977379967416, 0.1500387484187311, 1.455521528349477, 5.174151790129446, 6.185675158109706, -3.3445530715476366, -2.8387913875575066, 1.7328747098924515, -5.150844800083817, 1.8319294175863712, -12.557806236584437, -0.13219433416504706, 7.467559610619253, -3.558977379967416, 9.928777759437583, -5.355946312485345, -4.428328108669345, 5.738180949233645, -3.0905068800502904, 5.0203256558283, 1.1402944879823564, -8.721475675074048, 0.338825367788319, 0.338825367788319, -15.438550206224075, 1.2317744239114468, 8.199399098051977, 0.6240446584716932, 2.9063816586898277, 0.7644692734966028, 0.3105656423579949, 3.0205859099134056, 2.6569968652016067, -1.5067969064498588, -7.467559610619253, 0.4259352430838541, 15.373290634096314, 1.6326546526962507, -12.492546664456679, -1.3063567920574568, 22.281482483620493, 0.32862855964335663, -10.348303580258891, 12.362027520201162, 0.3114396544847059, -3.3189153824974458, -5.761487939279274, 5.5983390089598775, 5.859377297470912, -1.3599628691624015, 6.353485486438228, -1.4403719848198184, -8.9312385854847, -1.264404209975326, -4.088046054003175, -3.6801737282046822, -4.216234499254129, 3.3422223725430737, 0.598989644172643, 0.6805641093323413, -4.715004086230571, -1.7783233404814265, 14.543561788471955, 4.097368850021425, -6.274241720283094, 3.4004898476571443, -4.91311350161841, 5.052955441892179, -6.91751464554243, 7.169230138035213, -1.3016953940483311, -2.8644290766076974, -5.048294043883054, 4.67305150414844, 2.71759503932024, -9.03845073969459, -9.22956805806874, -1.072121542098894, -6.446713446620741, 10.049974107674851, 8.502389968645142, 1.4869859649110748, -6.283564516301344, -2.5637689050190944, 2.190857064289044, 3.824677066487576, 9.150324291913604, -0.5025569728588566, 11.495007490503795, -0.6537610707798691, -8.884624605393444, 8.227367486106731, -5.430528680631355, 0.7644692734966028, 1.396088703733125, 2.7129336413111145, -11.970470087434608, 4.020455782870853, 2.894728163667014, -2.236305694878019, -1.35879751966012, -14.562207380508456, -6.190336556118831, -4.866499521527154, -3.3259074795111343, -4.579823543965928, 5.197458780175074, 5.7475037452518976, 4.733649678267073, 0.15994421918812307, -3.6988193202411845, 2.0451883765038685, 5.346623516467094, 2.71759503932024, -3.745433300332441, -10.264398416094629, 10.413563152386649, -9.467299356534147, -5.28602534234846, 2.3540059946084413, 11.5322986745768, 0.32425849900980136, 2.085975609083718, -0.6759027113232158, -11.821305351142588, -2.232809646371175, -0.91130331078406, 2.310888063024029, -3.4890564098305314, 5.155506198092943, 3.8852752406062097, -4.323446653464019, -1.0400744307861554, 2.040526978494743, 3.5496545839491644, -5.374591904521847, 1.020846163998512, 0.60481639168405, -7.938360809540942, -2.1022905021156575, 5.7195353571971435, 2.5031707309004614, -6.003880635753807, 1.911173183741507, 1.6093476626506225, -3.498379205848783, 4.861838123518028, 2.5218163229369637, -1.5685604300707734, -2.2689354809418987, -1.9612832123396073, -0.9002324905123865, -5.691566969142389, -2.33652575207422, -7.7239365011211625, 0.037473269932736475, -4.202250305226753, 1.6070169636460596, -4.160297723144621, -1.9135038827460695, 3.391167051638893, -1.570891129075336, -0.730674137930442, -2.6080521861057884, -4.223226596267818, 1.0295862852656228, -2.416934867731637, 7.85445564537668, -10.348303580258891, 4.593807737993305, 3.022916608917969, 1.0610507218272207, -3.9435427157202803, 5.472481262713486, 7.9803133916230715, -6.511973018748501, 3.029908705931657, 2.482194439859396, 3.0555463949818478, -9.425346774452017, -2.8714211736213855, -3.712803514268561, 2.4472339547909536, -1.679268632787507, -1.9741020568647027, -8.716814277064922, 3.976172501784159, 0.820988724357251, 4.363068536541586, 8.609602122855032, 5.31865512841234, 3.2676400043970637, 0.17043236470865572, -2.1570619287228836, -3.675512330195556, -0.4856594050757762, 2.4192655667362, 5.686905571133264, 6.493327426711998, 1.4228917422855973, -4.381714128578088, -6.041171819826811, 2.4262576637498885, 5.052955441892179, -0.15615683330570848, -5.50977244678649, -0.43758873810666815, 5.472481262713486, 2.955326337785647, 2.275927577955587, -0.3726205033544797, -1.3075221415597382, 6.675121949067897, -3.896928735629024, -4.213903800249566, -1.0878537603796932, 2.2782582769601496, -12.222185579927391, 3.6731816311909937, 1.0552239743158136, 2.0265427844673662, 0.13758407561309857, 4.0460934719210435, -6.525957212775877, -8.022265973705203, 2.8387913875575066, -0.2608926198232499, -7.817164461303675, -3.102160375073104, -0.10961568755834482, -7.840471451349304, 4.15097492712637, -10.27372121211288, 8.618924918873283, -0.5523756640813867, 0.08019061262573929, 0.026074695113546473, 4.297808964413827, 4.185935412194812, 6.987435615679313, 8.278642864207113, 5.547063630859496, 11.01022209755473, -7.262458098217726, -2.4379111587727027, 5.080923829946933, -0.5404308316830023, -1.6408120992122206, 2.40295067370426, 7.509512192701385, 2.5637689050190944, -0.8297288456243614, -2.6546661661970443, 0.541013506434143, -0.8093352293344369, -0.21748335086326753, -4.3770527305689635, -2.0766528130654667, 7.276442292245102, -6.348824088429104, 2.743232728370431, 1.215459530879507, -7.3323790683546095, -4.87116091953628, -1.5930327696186828, 3.4867257108259686, 9.206261068023112, -5.122876412029064, -6.824286685359917, -0.7866109140399495, -5.654275785069385, -1.5289385469932055, -3.4377810317301494, 1.3483093741395873, -5.225427168229827, -8.926577187475575, 4.715004086230571, -4.631098922066309, 5.593677610950751, -2.1279281911658483, -4.286155469391014, 5.947943859644299, 2.8970588626715763, 3.9854952978024105, 1.3366558791167735, -0.7021230751245475, 2.295738519494371, -5.267379750311958, 3.7617481933643804, -4.4376509046875965, -7.043372391788822, -2.061503269535808, 0.9450984463502208, 9.411362580424639, 2.9949482208632148, 0.49294283946503503, 2.0941330555996878, 6.661137755040521, 8.698168685028419, 2.356336693613004, 1.8062917285361801, 1.0569719985692358, -0.23379824389520718, 8.847333421320439, 5.425867282622229, 11.215323609956256, 6.647153561013143, -10.133879271839113, -0.8477917629097232, -4.346753643509647, -0.09971021678895285, -2.0673300170472153, -1.8097877770430242, 1.8983543392164113, 3.0998296760685418, 4.337430847491396, -2.7945081064708126, 6.143722576027576, -5.13686060605644, -0.4760452716819546, 4.638091019079998, 2.698949447283738, 6.912853247533304, 3.9248971236837775, -5.411883088594852, -2.8644290766076974, -1.6279932546871252, 0.5127537810038189, 0.5485882781989722, 4.129998636085305, 5.654275785069385, 0.2884240018146481, -0.7551464774783514, 10.33898078424064, 0.24137301566003633, -2.0708260655540593, 1.5242771489840798, -1.8016303305270545, 8.190076302033725, 6.278903118292218, -1.5429227410205824, 11.345842754211775, 5.971250849689928, -7.164568740026088, -4.232549392286068, 5.5983390089598775, -6.2462733322283395, -2.229313597864331, 8.17609210800635, 1.9333148242848535, 3.4890564098305314, -4.521556068851858, 0.5727692803713114, 1.8680552521570948, 1.4485294313357882, 2.421596265740763, -0.677650735576638, -0.8565318841768338, -12.073020843635371, -0.28827833312686296, -4.458627195728662, -1.3681203156783714, -1.255081413957075, -1.5114583044589844, 0.6345328039922259, 0.6828948083369042, 1.8995196887186927, -6.460697640648117, -0.5552890378370903, 0.1281884452509547, 3.202380432269305, 1.1927352155850195, 10.469499928496157, -3.4750722158031544, 8.851994819329564, -2.6220363801331645, 2.885405367648763, -1.6850953802989137, -1.9741020568647027, 4.204581004231315, 6.241611934219213, -1.386765907714874, -0.48682475457805763, -3.316584683492883, -5.603000406969003, 8.7494440631288, -2.556776808005406, 0.41544709756332143, 11.336519958193524, 4.099699549025988, -4.24187218830432, -9.457976560515895, 1.056389323818095, 0.9876337031834921, 0.25958160163318333, 1.785315437495115, -3.4843950118214058, 0.2588532581942574, 3.353875867565888, -6.856916471423796, 2.041692327997024, 5.864038695480039, 0.08543468538600563, 0.8810042237247434, 0.4061243015450702, 1.2877112000209543, 5.36060771049447, -2.1826996177730744, -10.27372121211288, 0.46672247566370334, -7.211182720117344, 4.2348800912906315, 2.952995638781084, 2.5637689050190944, -5.458497068686109, 1.5452534400251452, -2.317880160037718, 2.1081172496270644, 8.875301809375193, 4.029778578889104, 1.2504200159479493, -10.049974107674851, 1.5638990320616477, -3.1534357531734862, -3.3492144695567627, -7.486205202655755, 3.2513251113651243, -2.9879561238495267, 2.6826345542517984, 0.3306679212723491, -6.810302491332541, 0.24923912480043584, -7.4395912225645, -6.278903118292218, 2.1279281911658483, 6.031849023808561, -2.4868558378685215, 9.406701182415514, -1.144955885991482, 2.2188254523437982, 0.9812242809209444, -0.04981869122253013, -1.6373160507053763, 1.1286409929595422, -10.115233679802609, 3.202380432269305, -4.1672898201583095, 1.8400868641023411, -0.16212924950490068, 2.9320193477400194, 7.416284232518872, -4.325777352468581, 1.3459786751350247, -6.148383974036701, 2.4425725567818284, 2.27010083044418, 4.24187218830432, 0.4935255142161757, 6.572571192867133, 1.6000248666323713, 11.588235450686305, 3.2886162954381293, 1.6641190892578486, -4.738311076276199, 1.5487494885319895, 0.7551464774783514, -6.47002043666637, 6.362808282456481, -4.668390106139315, -7.3603474564093645, -0.6922176043551554, 3.8293384644967023, 6.3208557003743495, 0.8407996658960348, -0.6828948083369042, 4.708011989216883, -6.185675158109706, -6.372131078474731, -2.341187150083346, 3.8619682505605812, 12.380673112237663, 2.3062266650149037, -2.780523912443436, -0.7347528611884269, 0.5305253609136104, -7.640031336956902, -0.8180753506015475, 6.013203431772059, 10.003360127583594, -0.38106928724601996, 4.0321092778936665, 7.7239365011211625, 0.7446583319578188, 5.621645999005506, 3.5869457680221695, -1.9216613292620393, -1.188073817575894, 4.537870961883797, 1.3646242671715272, -0.9066419127749343, -6.6844447450861475, 2.554446109000843, 0.5631551469774897, 1.2434279189342607, -5.052955441892179, 2.2561166364168033, 0.48799010408033905, 1.3436479761304618, 1.241097219929698, -12.2781223560369, -1.3308291316053664, 1.6058516141437782, 0.3758252144857537, 2.2188254523437982, -9.22956805806874, 0.28172324217653005, -4.367729934550712, -2.280588975964713, -3.7034807182503098, 6.13906117801845, 4.297808964413827, -4.717334785235134, -5.938621063626048, 1.3110181900665823, 13.284984326008036, 1.005696620468854, 7.29042648627248, -4.885145113563657, -2.645343370178793, 6.372131078474731, -0.22942818326165193, -6.101769993945445, 11.839950943179092, 0.2234557670624597, -5.267379750311958, 3.9458734147248427, -4.731318979262511, -6.181013760100581, 4.265179178349948, 4.195258208213064, -1.4916473629202005, -0.33591199403261546, 4.474942088760601, 2.8667597756122603, 1.7771579909791453, 6.824286685359917, -3.4307889347164613, -1.4263877907924416, -3.209372529282994, 4.945743287682289, 8.875301809375193, 1.2655695594776075, 2.880743969639637, -2.9716412308175864, -3.633559748113426, -12.110312027708376, 1.7806540394859893, -3.507702001867034, 7.117954759934832, -0.7271780894235977, 3.691827223227496, -2.9273579497308932, 2.7665397184160594, 9.178292679968358, 2.7222564373293654, -0.7743747442659947, 7.4209456305279975, -4.479603486769727, -1.4485294313357882, -2.4239269647453257, 4.703350591207757, 5.31865512841234, -1.2748923554958589, 2.5894065940692856, 1.619835808171155, -0.642107575757055, -8.63290911290066, 2.2514552384076776, 2.8248071935301295, 5.076262431937807, 1.8517403591251549, 13.872320475157863, 0.7038710993779695, 3.4471038277484007, -1.8062917285361801, 1.6804339822897882, 0.6980443518665626, -3.9039208326427124, 5.980573645708178, 2.3283683055582505, -6.204320750146208, -1.1700109002905321, 4.111353044048802, 3.5100327008715966, -9.854195391291574, 2.052180473517557, 7.164568740026088, -2.3633287906266927, -1.5557415855456778, -3.9248971236837775, -4.323446653464019, -2.73390993235218, -3.4471038277484007, -3.2862855964335664, -3.717464912277687, 11.467039102449041, -2.5474540119871545, -3.179073442223677, 9.350764406306007, 2.710602942306552, 3.1254673651187326, -6.386115272502107, -4.782594357362893, -11.47636189846729, -6.409422262547736, -6.665799153049646, 5.514433844795617, 8.40916200846263, 1.017350115491668, 2.73390993235218, -3.118475268105044, -0.8075872050810147, 3.1883962382419284, -1.1437905364892005, -2.2864157234761198, -2.9949482208632148, 1.753851000933517, 3.176742743219114, -1.8505750096228737, 7.071340779843576, -3.607922059063235, 10.823766177189704, 2.1710461227502607, -0.4955648758451681, -2.1197707446498786, 2.47753304185027, -4.335100148486832, 6.740381521195655, -8.432468998508257, -10.805120585153203, 0.8262327971175172, 8.287965660225364, -3.0998296760685418, -4.484264884778852, 4.328108051473144, 3.4051512456662705, -2.51016282791415, -5.304670934384963, -0.40554162679392947, -3.491387108835094, 0.6077297654397535, 14.664758136709219, -1.7130637683536676, -0.07778707927728389, -8.157446515969847, -8.698168685028419, -0.37553387711018327, -2.468210245832019, 3.0532156959772854, -8.637570510909786, 4.015794384861727, -2.0906370070928433, 3.8876059396107725, 3.7617481933643804, -2.1745421712571047, 3.235010218333185, 3.5799536710084814, -4.132329335089867, 14.515593400417199, 4.3560764395278975, -4.866499521527154, 2.5800837980510343, 7.122616157943957, -5.313993730403213, -3.242002315346873, 3.540331787930913, -3.8106928724601996, 2.0743221140609034, 4.176612616176561, -1.865724553152532, -6.4886660287028715, -2.3691555381380995, 3.57063087499023, 0.2654083491445903, -1.2329397734137282, -0.328919897018927, -7.532819182747012, 12.100989231690127, -5.272041148321084, 3.433119633721024, 4.3560764395278975, 1.1321370414663865, -1.0727042168500347, 2.077818162567748, -4.286155469391014, -3.3981591486525815, 2.243297791891708, 9.994037331565343, 3.915574327665526, 5.556386426877746, -3.1557664521780495, 3.7104728152639987, 6.3068715063469725, 3.542662486935476, -8.48374437660864, 6.4280678545842385, 3.86896034757427, -3.3002697904609435, -1.2282783754046025, -4.3770527305689635, -7.901069625467937, -4.274501974368199, 1.6664497882624114, -11.485684694485542, -9.192276873995734, 3.2862855964335664, 0.45244694426075605, 0.8996498157612459, -3.5985992630449837, 2.855106280589446, 3.815354270469325, -3.1650892481963004, 5.076262431937807, 11.77469137105133, 2.004401143924019, 0.9911297516903362, 2.71759503932024, -7.462898212610128, 3.71979561128225, -6.278903118292218, -1.734040059394733, 0.1984007527634095, -0.9596653151287383, -1.3774431116966226, 3.3352302755293857, -8.768089655165303, -3.938881317711154, 1.7118984188513864, 4.423666710660219, 4.4516350987149735, -2.6639889622152957, -5.742842347242771, 3.6848351262138075, 0.7737920695148539, -2.3458485480924716, -8.040911565741705, 7.560787570801766, -0.1777157990979145, 4.393367623600903, 4.885145113563657, 2.210668005827828, 4.367729934550712, -2.848114183575758, 0.7504850794692258, 1.0493972268044067, 8.124816729905968, 4.237210790295195, 1.1968139388430046, -5.0249870538374255, -6.129738382000198, -6.908191849524178, -0.5512103145791053, 0.9293662280694217, 1.802795680029336, 2.754886223393245, 0.44137612398908277, -5.122876412029064, 4.691697096184943, -4.959727481709667, 5.8687000934891635, 3.9062515316472752, -2.1780382197639487, -3.726787708295938, 4.30013966341839, -3.8083621734556368, -0.8909096944941354, -3.528678292908099, -3.600929962049546, -5.626307397014631, 0.36300636996065816, 10.72121542098894, 1.8867008441935973, -3.9039208326427124, -11.084804465700739, 13.85367488312136, -2.9063816586898277, -0.2349635933974886, -5.006341461800923, 3.491387108835094, -8.17609210800635, -2.8131536985073153, 1.9752674063669842, 2.7292485343430544, 1.965944610348733, 2.6896266512654865, -4.099699549025988, 0.49935226172758274, -7.7239365011211625, 4.102030248030551, -2.629028477146853, 3.71979561128225, 5.519095242804742, -3.083514783036602, -0.6065644159374721, -2.482194439859396, 1.9309841252802906, 1.2061367348612557, -0.7650519482477435, 4.160297723144621, -2.5218163229369637, 7.276442292245102, -7.164568740026088, 0.45506898064088924, -3.8503147555377675, 4.787255755372018, 5.3885760985492235, 3.9901566958115358, 2.7875160094571245, -1.8342601165909338, -1.4636789748654466, -2.3225415580468436, 2.4192655667362, -2.004401143924019, -2.172211472252542, -7.080663575861826, 2.5427926139780292, 7.024726799752319, 3.3002697904609435, -10.823766177189704, -9.364748600333384, -2.0102278914354264, 6.800979695314289, -0.05375174579272988, 8.050234361759957, 1.5114583044589844, 2.164054025736572, 5.831408909416158, 0.9538385676173312, -9.844872595273323, -1.3110181900665823, 4.006471588843476, -4.708011989216883, 8.66553889896454, -22.132317747328475, -4.330438750477707, -4.945743287682289, 5.6589371830785105, -8.754105461137927, 4.712673387226008, 0.5214939022709295, 2.95765703679021, -0.8722641024576329, -4.428328108669345, -1.5126236539612659, -11.15938683384675, -2.040526978494743, 8.437130396517384, 4.349084342514209, 2.250289888905396, -8.805380839238309, 3.6475439421408025, 4.0927074520122995, -4.875822317545405, -3.7221263102868125, -0.584131438018555, -2.181534268270793, 0.6823121335857636, -6.77767270526866, 0.32600652326322355, -7.201859924099092, 6.800979695314289, -0.8349729183846278, 0.4303053037174094, -10.096588087766108, -1.6373160507053763, -3.0765226860229133, -0.419234483445736, -3.9621883077567825, 5.06227823791043, 2.9996096188723405, 2.181534268270793, -7.947683605559193, 0.9567519413730348, -1.639646749709939, -3.5613080789719787, -5.761487939279274, -5.253395556284581, 8.138800923933344, 9.658416674908297, 3.5636387779765415, -0.7201859924099092, -4.2348800912906315, 3.083514783036602, -1.7247172633764818, -0.9631613636355825, 1.4357105868106927, -4.80590134740852, -3.0905068800502904, -6.745042919204781, 4.927097695645787, -7.55612617279264, 2.8970588626715763, 3.083514783036602, -8.073541351805584, -0.46031305340115564, 3.9179050266700886, -5.16949039212032, -8.996498157612459, 4.7616180663218275, -4.526217466860984, -4.829208337454149, -1.96361391134417, -0.30386488271987677, 5.822086113397908, 2.6127135841149136, -9.723676247036057, -2.6220363801331645, 1.329663782103085, -10.320335192204137, -3.8013700764419482, 3.6288983501042997, 5.057616839901304, 6.6844447450861475, 2.666319661219858, 2.1407470356909437, -11.075481669682487, -0.43234466534640187, 0.3149357029915501, 0.48944679095819077, 3.6685202331818676, -1.4951434114270445, 11.83062814716084, 0.10845033805606341, 2.1011251526133763, -14.757986096891733, -6.194997954127958, 4.521556068851858, -2.244463141393989, -4.526217466860984, -0.9363583250831102, 1.2480893169433864, -7.201859924099092, 0.561698460099638, 3.715134213273124, -1.82260662156812, 4.777932959353767, -3.351545168561325, 3.0322394049362202, -2.587075895064723, -1.4427026838243815, -2.8900667656578882, 0.41719512181674356, 0.8070045303298741, -0.03024446130139713, -3.258317208378813, 9.290166232187374, -1.131554366715246, 5.197458780175074, -1.1787510215576427, -7.7845346752397955, -4.509902573829043, 0.7825321907819645, 10.693247032934188, 6.283564516301344, 1.2212862783909142, -2.95765703679021, -0.7166899439030651, 6.292887312319596, 2.3912971786814463, 2.5008400318958985, 2.668650360224421, -5.714873959188019, -9.00582095363071, 0.6013203431772058, 10.646633052842931, -7.621385744920399, 5.4165444866039785, 1.8249373205726827, -10.767829401080197, 3.6288983501042997]
    image_embedding_tensor = torch.tensor(image_embedding_list)
    normalized_tensor = torch.nn.functional.normalize(image_embedding_tensor, p=2, dim=0)
    image_embedding_tensor = torch.unsqueeze(image_embedding_tensor, 0)
    negative_image_embedding = None
    prompt_generation_policy = "top-k"

    # Generate img2img generation jobs with Kandinsky using the embeddings
    response = generate_img2img_generation_jobs_with_kandinsky(
        image_embedding=image_embedding_tensor,
        negative_image_embedding = None,  
        dataset_name="test-generations",
        prompt_generation_policy=prompt_generation_policy
    )

    print(response)