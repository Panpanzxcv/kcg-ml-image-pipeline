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
    image_embedding_list = [-6.484936910295572, 1.9391415717962606, -7.607401550893022, -3.4196015794945596, 2.3418863597847146, 3.42519525710551, -1.7890445559024153, 1.8076901479389178, -8.293559337836314, -3.117542988503219, -3.2406038959441354, -7.4806115250448055, 1.7657375658567873, 1.4674080932727471, 4.042364353513743, 3.952865511738531, -1.868288322057551, 1.1485684694485543, 3.179073442223677, 0.129237259803008, -2.938545304952795, 1.0040651311656599, -4.322048234061281, -2.643944950776055, 2.4164687279307246, -0.2920365852717205, 3.923032564480127, -4.303402642024778, -5.757758820871974, 0.8702830083037545, -2.2300128075656995, -8.882760046189794, -3.290946994442692, -4.646481535496425, 3.714201933671299, 0.29413421437582704, 2.2859495836752073, -0.4288486168395576, 1.205437525159887, 6.287293634708645, 3.833533722704915, 1.160688104272281, -1.7498888126257601, -0.8977852565575957, 2.955326337785647, 1.4413042644216436, -10.411698593182999, -4.866499521527154, -0.43747220315644003, -4.124404958474354, 1.7228527041728314, -6.190336556118832, -4.273569694766374, -2.5507169905935427, 0.6162368168064078, 2.9497326601746963, -6.044900938234112, -1.3518054226464316, 0.855832674475465, 2.099493663310182, -1.1672140614850568, -0.1573221828079899, 15.70704673154971, -2.067796156848128, 1.1858596535215593, 0.37011500192457475, 1.5531778166406587, -1.7303109409874324, -3.6489423615435403, 1.4413042644216436, 3.669452512783693, 2.8471819039739326, 5.414679927400328, 0.09835549799255072, -3.6265676510997373, 1.295868646536924, 5.679447334318663, 1.0460177132477906, 3.87082490677792, -8.136936364729694, -5.910652675571294, -1.1485684694485543, -3.6228385326924366, -0.36242369520951745, -1.6277601847866687, 3.8969287356290234, 9.859789068902526, 5.944214741236999, 2.1554304394196895, 4.232549392286069, -0.4871160919536279, 0.7952345003568319, -6.041171819826812, 2.351209155802966, -3.7235247296895504, 0.15149543529658285, -2.134920288179537, 3.246197573555086, -5.88081972831289, -5.217036651813401, -3.878283143592521, -3.468080118789466, -1.422658672385141, -5.366201388105421, 7.96539691799387, -0.25497847109917177, -3.2256874223149334, 1.034830358025889, -7.0741376186490506, 2.733443792551267, -3.7552222361516048, -4.4637547335387, 2.8434527855666323, -4.515962391240906, -0.9495267744588901, -1.9708390782583147, 6.731058725177404, 1.1895887719288598, 1.0665278644879432, -1.1206000813938006, 1.9894846702948172, -3.8670957883706194, -1.965245400647364, 3.458757322771215, -3.468080118789466, -0.2596398691082974, -0.3729118407300501, 4.598002996201518, 4.7397094956789365, 0.990080937138283, 0.27945081064708127, -4.329506470875882, 1.355534541053732, -0.9910132167401081, -4.668856245940227, -6.056088293456014, 0.07621385744920399, 0.8511712764663394, -2.4164687279307246, -5.172287230925795, 11.545350589002352, 9.136340097886228, -1.355534541053732, -1.2156926007799633, -3.2890824352390418, -5.936756504422398, 7.499257117081307, 4.560711812128512, -1.377909251497535, 2.8322654303447306, -2.6178411219249518, -2.4164687279307246, -0.8026927371714329, 10.158118541486566, -3.8298046042976144, 2.1684823538452416, -1.390961165923087, -3.067199890004662, 5.448241993066032, -1.7098007897472798, -0.0964326713137864, -0.3344553071547637, 3.067199890004662, -4.0610099455502455, 3.5072358620661213, -4.005073169440738, 1.60072407633374, -3.594870144637683, -3.762680472966206, 0.1107664701918477, -1.6258956255830184, -0.16850953802989138, -0.18272680195772456, 1.4795277280964738, -4.303402642024778, -1.7881122763005903, 4.366797654948887, -3.074658126819263, 2.629028477146853, -0.997539173952884, -3.7869197426136587, 5.5974067293580525, 0.08547838599234117, -5.888277965127491, -1.2716293768894709, 0.4255856382331697, 3.2741659616098397, -1.3974871231358628, -0.975164463509081, 3.119407547706869, -3.6135157366741857, 1.6389475400085702, 4.955998363302366, 6.40289630533496, -0.7742582093157665, -1.0767829401080198, -4.609190351423419, -2.164753235437941, -4.336964707690483, 11.247021116418312, -1.0180493251930367, 0.5355946312485345, -14.677810051134772, -1.7955705131151913, -1.1709431798923573, 2.7725995358279225, -0.7225166914144721, -3.781326065002708, 21.79296797226413, -1.5438550206224075, -3.9342199197020284, 3.4923193884369192, 0.12376011714228538, -3.4718092371967666, 3.4196015794945596, -0.7695968113066409, -3.242468455147786, -4.519691509648207, -1.2156926007799633, 0.23167148105354363, 4.150508787325458, -1.6874260793034768, -9.121423624257025, 2.725985555736666, 3.2928115536463425, -2.2934078204898083, -1.140177953032128, 0.65119730187485, 9.561459596318485, -4.750896850900839, 10.202867962374171, 0.5258056954293706, -0.5756826541270148, 1.325701593795328, -0.4724326882248822, 3.8372628411122154, -1.816080664355344, 6.455103963037168, 1.7610761678476616, -2.8210780751228293, -0.878207384919268, -4.31458999724668, -5.836070307425284, 2.560039786611794, 4.310860878839379, -1.8748142792703268, -3.1846671198346277, 0.8096848341851213, 4.672585364347528, 6.119483306380122, 0.5020325655828299, 11.351436431822725, -8.651554704937162, 5.041768086670277, -3.915574327665526, -6.332043055596251, -5.2207657702207015, 1.376044692293885, -6.540873686405079, 0.6181013760100581, 0.3568300175985667, -6.481207791888271, -5.179745467740396, -0.5696228367151516, -8.241351680134107, -6.47002043666637, -7.59248507726382, 1.2688325380839955, 4.624106825052621, -3.302134349664594, -9.6733331485375, 0.8427807600499132, -1.362060498266508, 2.655132305997957, -3.5258814541026235, -2.269168550842355, -6.033713583012211, 7.040575552983346, 3.9342199197020284, 5.254327835886406, -6.462562199851768, 4.116946721659753, -1.8841370752885782, 3.0410960611535587, 6.186607437711531, -1.525209428585905, 4.922436297636661, 1.0889025749317462, 5.120079573223588, 6.9697223032446365, 1.5093606753548778, -15.811462046954125, -5.142454283667391, -4.508504154426306, -0.8726137073083172, -3.6564005983581414, 1.8431167728082727, 0.21255974921612855, -3.94540727492393, 1.8076901479389178, 2.8471819039739326, 0.12003099873498488, 1.1644172226795815, 4.139321432103556, 4.948540126487765, -2.6756424572381095, -2.2710331100460053, 1.3862997679139613, -4.120675840067054, 1.465543534069097, -10.04624498926755, -0.10575546733203765, 5.974047688495403, -2.8471819039739326, 7.943022207550067, -4.284757049988276, -3.542662486935476, 4.590544759386916, -2.4724055040402324, 4.0162605246626395, 0.9122355903858851, -6.977180540059238, 0.2710602942306552, 0.2710602942306552, -12.35084016497926, 0.9854195391291574, 6.559519278441582, 0.49923572677735456, 2.3251053269518622, 0.6115754187972822, 0.2484525138863959, 2.4164687279307246, 2.1255974921612855, -1.205437525159887, -5.974047688495403, 0.34074819446708327, 12.298632507277052, 1.3061237221570006, -9.994037331565343, -1.0450854336459654, 17.825185986896393, 0.2629028477146853, -8.278642864207113, 9.88962201616093, 0.24915172358776472, -2.655132305997957, -4.609190351423419, 4.478671207167902, 4.68750183797673, -1.0879702953299213, 5.082788389150583, -1.1522975878558548, -7.14499086838776, -1.0115233679802609, -3.2704368432025395, -2.9441389825637456, -3.372987599403303, 2.673777898034459, 0.4791917153381144, 0.5444512874658731, -3.7720032689844567, -1.422658672385141, 11.634849430777564, 3.2778950800171405, -5.019393376226475, 2.7203918781257155, -3.930490801294728, 4.042364353513743, -5.534011716433944, 5.73538411042817, -1.041356315238665, -2.291543261286158, -4.038635235106443, 3.7384412033187524, 2.174076031456192, -7.230760591755671, -7.383654446454992, -0.8576972336791152, -5.157370757296593, 8.03997928613988, 6.801911974916114, 1.1895887719288598, -5.026851613041075, -2.0510151240152754, 1.7526856514312354, 3.059741653190061, 7.320259433530883, -0.40204557828708526, 9.196005992403036, -0.5230088566238953, -7.107699684314755, 6.581893988885384, -4.344422944505084, 0.6115754187972822, 1.1168709629865001, 2.1703469130488915, -9.576376069947687, 3.216364626296682, 2.3157825309336113, -1.7890445559024153, -1.087038015728096, -11.649765904406765, -4.952269244895065, -3.893199617221723, -2.6607259836089074, -3.6638588351727424, 4.157967024140059, 4.598002996201518, 3.7869197426136587, 0.12795537535049845, -2.9590554561929476, 1.636150701203095, 4.277298813173675, 2.174076031456192, -2.9963466402659527, -8.211518732875703, 8.33085052190932, -7.573839485227317, -4.228820273878768, 1.883204795686753, 9.22583893966144, 0.2594067992078411, 1.6687804872669743, -0.5407221690585726, -9.45704428091407, -1.78624771709694, -0.729042648627248, 1.8487104504192233, -2.7912451278644252, 4.124404958474354, 3.1082201924849677, -3.458757322771215, -0.8320595446289243, 1.6324215827957944, 2.8397236671593316, -4.299673523617478, 0.8166769311988097, 0.48385311334724, -6.3506886476327535, -1.681832401692526, 4.575628285757714, 2.002536584720369, -4.803104508603045, 1.5289385469932055, 1.287478130120498, -2.7987033646790263, 3.8894704988144224, 2.017453058349571, -1.2548483440566187, -1.8151483847535188, -1.5690265698716859, -0.7201859924099092, -4.553253575313912, -1.8692206016593762, -6.17914920089693, 0.029978615946189183, -3.361800244181402, 1.2856135709168477, -3.328238178515697, -1.5308031061968557, 2.7129336413111145, -1.2567129032602689, -0.5845393103443536, -2.0864417488846305, -3.378581277014254, 0.8236690282124982, -1.9335478941853097, 6.283564516301344, -8.278642864207113, 3.6750461903946436, 2.418333287134375, 0.8488405774617765, -3.154834172576224, 4.3779850101707884, 6.384250713298457, -5.2095784149988, 2.4239269647453257, 1.9857555518875167, 2.4444371159854783, -7.540277419561614, -2.2971369388971086, -2.970242811414849, 1.957787163832763, -1.3434149062300056, -1.5792816454917622, -6.973451421651937, 3.1809380014273274, 0.6567909794858008, 3.490454829233269, 6.887681698284026, 4.254924102729872, 2.614112003517651, 0.13634589176692458, -1.725649542978307, -2.940409864156445, -0.38852752406062097, 1.93541245338896, 4.549524456906611, 5.194661941369598, 1.138313393828478, -3.505371302862471, -4.832937455861449, 1.9410061309999107, 4.042364353513743, -0.12492546664456679, -4.4078179574291925, -0.3500709904853345, 4.3779850101707884, 2.3642610702285176, 1.8207420623644697, -0.2980964026835838, -1.0460177132477906, 5.340097559254318, -3.117542988503219, -3.371123040199653, -0.8702830083037545, 1.8226066215681198, -9.777748463941913, 2.938545304952795, 0.844179179452651, 1.621234227573893, 0.11006726049047885, 3.2368747775368347, -5.2207657702207015, -6.417812778964162, 2.2710331100460053, -0.20871409585859993, -6.25373156904294, -2.4817283000584833, -0.08769255004667585, -6.272377161079443, 3.320779941701096, -8.218976969690305, 6.895139935098626, -0.44190053126510936, 0.06415249010059143, 0.020859756090837178, 3.438247171531062, 3.34874832975585, 5.589948492543451, 6.6229142913656895, 4.4376509046875965, 8.808177678043783, -5.8099664785741805, -1.950328927018162, 4.064739063957546, -0.43234466534640187, -1.3126496793697764, 1.9223605389634082, 6.007609754161107, 2.0510151240152754, -0.6637830764994892, -2.1237329329576355, 0.4328108051473144, -0.6474681834675495, -0.17398668069061402, -3.5016421844551706, -1.6613222504523733, 5.821153833796082, -5.079059270743283, 2.194586182696345, 0.9723676247036056, -5.865903254683688, -3.8969287356290234, -1.2744262156949462, 2.789380568660775, 7.365008854418489, -4.098301129623251, -5.459429348287934, -0.6292887312319596, -4.523420628055508, -1.2231508375945643, -2.7502248253841195, 1.07864749931167, -4.180341734583862, -7.141261749980459, 3.7720032689844567, -3.7048791376530477, 4.474942088760601, -1.7023425529326788, -3.428924375512811, 4.758355087715439, 2.3176470901372612, 3.1883962382419284, 1.0693247032934188, -0.561698460099638, 1.8365908155954969, -4.213903800249566, 3.0093985546915043, -3.550120723750077, -5.634697913431057, -1.6492026156286466, 0.7560787570801766, 7.529090064339711, 2.395958576690572, 0.394354271572028, 1.67530644447975, 5.328910204032416, 6.958534948022735, 1.8850693548904034, 1.4450333828289441, 0.8455775988553886, -0.18703859511616575, 7.077866737056351, 4.340693826097783, 8.972258887965005, 5.317722848810514, -8.10710341747129, -0.6782334103277786, -3.4774029148077172, -0.07976817343116228, -1.6538640136377722, -1.4478302216344194, 1.518683471373129, 2.4798637408548334, 3.4699446779931162, -2.23560648517665, 4.914978060822061, -4.109488484845152, -0.38083621734556367, 3.7104728152639987, 2.15915955782699, 5.530282598026643, 3.139917698947022, -4.329506470875882, -2.291543261286158, -1.3023946037497, 0.4102030248030551, 0.4388706225591777, 3.3039989088682438, 4.523420628055508, 0.2307392014517185, -0.6041171819826812, 8.271184627392511, 0.19309841252802906, -1.6566608524432476, 1.2194217191872638, -1.4413042644216436, 6.55206104162698, 5.023122494633775, -1.2343381928164658, 9.07667420336942, 4.777000679751942, -5.73165499202087, -3.386039513828855, 4.478671207167902, -4.997018665782671, -1.7834508782914646, 6.540873686405079, 1.5466518594278829, 2.7912451278644252, -3.617244855081486, 0.4582154242970491, 1.4944442017256758, 1.1588235450686306, 1.9372770125926102, -0.5421205884613104, -0.685225507341467, -9.658416674908297, -0.23062266650149035, -3.5669017565829293, -1.0944962525426971, -1.0040651311656599, -1.2091666435671875, 0.5076262431937807, 0.5463158466695234, 1.5196157509749542, -5.168558112518494, -0.4442312302696722, 0.10255075620076377, 2.561904345815444, 0.9541881724680157, 8.375599942796926, -2.7800577726425235, 7.081595855463651, -2.097629104106532, 2.3083242941190103, -1.348076304239131, -1.5792816454917622, 3.363664803385052, 4.993289547375371, -1.1094127261718991, -0.3894598036624461, -2.6532677467943064, -4.4824003255752025, 6.9995552505030405, -2.0454214464043248, 0.33235767805065713, 9.069215966554818, 3.279759639220791, -3.393497750643456, -7.566381248412717, 0.845111459054476, 0.7901069625467937, 0.20766528130654666, 1.428252349996092, -2.7875160094571245, 0.20708260655540595, 2.6831006940527105, -5.485533177139037, 1.6333538623976194, 4.691230956384031, 0.0683477483088045, 0.7048033789797947, 0.32489944123605613, 1.0301689600167634, 4.288486168395576, -1.7461596942184596, -8.218976969690305, 0.37337798053096266, -5.768946176093875, 3.387904073032505, 2.3623965110248673, 2.0510151240152754, -4.366797654948887, 1.2362027520201162, -1.8543041280301742, 1.6864937997016516, 7.100241447500154, 3.223822863111283, 1.0003360127583594, -8.03997928613988, 1.2511192256493182, -2.522748602538789, -2.67937157564541, -5.988964162124605, 2.6010600890920994, -2.3903648990796214, 2.1461076434014386, 0.2645343370178793, -5.448241993066032, 0.19939129984034867, -5.9516729780516, -5.023122494633775, 1.7023425529326788, 4.825479219046849, -1.9894846702948172, 7.525360945932412, -0.9159647087931856, 1.7750603618750385, 0.7849794247367555, -0.039854952978024105, -1.309852840564301, 0.9029127943676338, -8.092186943842087, 2.561904345815444, -3.333831856126648, 1.4720694912818728, -0.12970339960392055, 2.3456154781920153, 5.933027386015097, -3.460621881974865, 1.0767829401080198, -4.918707179229361, 1.9540580454254626, 1.816080664355344, 3.393497750643456, 0.39482041137294055, 5.258056954293706, 1.280019893305897, 9.270588360549045, 2.6308930363505034, 1.331295271406279, -3.7906488610209594, 1.2389995908255915, 0.6041171819826812, -5.1760163493330955, 5.090246625965184, -3.7347120849114517, -5.888277965127491, -0.5537740834841244, 3.0634707715973617, 5.056684560299479, 0.6726397327168279, -0.5463158466695234, 3.766409591373506, -4.948540126487765, -5.097704862779785, -1.8729497200666767, 3.089574600448465, 9.90453848979013, 1.8449813320119228, -2.224419129954749, -0.5878022889507415, 0.4244202887308883, -6.112025069565521, -0.6544602804812379, 4.810562745417647, 8.002688102066875, -0.30485542979681596, 3.2256874223149334, 6.17914920089693, 0.5957266655662551, 4.4973167992044045, 2.8695566144177356, -1.5373290634096315, -0.9504590540607152, 3.6302967695070376, 1.0916994137372218, -0.7253135302199475, -5.347555796068918, 2.0435568872006744, 0.4505241175819918, 0.9947423351474086, -4.042364353513743, 1.8048933091334425, 0.3903920832642712, 1.0749183809043694, 0.9928777759437584, -9.82249788482952, -1.064663305284293, 1.2846812913150225, 0.3006601715886029, 1.7750603618750385, -7.383654446454992, 0.22537859374122404, -3.4941839476405696, -1.8244711807717702, -2.962784574600248, 4.91124894241476, 3.438247171531062, -3.773867828188107, -4.750896850900839, 1.048814552053266, 10.627987460806429, 0.8045572963750831, 5.832341189017984, -3.908116090850925, -2.1162746961430345, 5.097704862779785, -0.18354254660932154, -4.881415995156356, 9.471960754543273, 0.17876461364996776, -4.213903800249566, 3.156698731779874, -3.785055183410009, -4.944811008080465, 3.4121433426799586, 3.356206566570451, -1.1933178903361603, -0.26872959522609235, 3.579953671008481, 2.2934078204898083, 1.4217263927833161, 5.459429348287934, -2.744631147773169, -1.1411102326339533, -2.567498023426395, 3.9565946301458315, 7.100241447500154, 1.012455647582086, 2.3045951757117096, -2.3773129846540693, -2.9068477984907406, -9.688249622166701, 1.4245232315887915, -2.8061616014936273, 5.694363807947865, -0.5817424715388781, 2.953461778581997, -2.3418863597847146, 2.2132317747328476, 7.342634143974687, 2.1778051498634925, -0.6194997954127958, 5.936756504422398, -3.5836827894157817, -1.1588235450686306, -1.9391415717962606, 3.762680472966206, 4.254924102729872, -1.019913884396687, 2.0715252752554285, 1.295868646536924, -0.513686060605644, -6.906327290320528, 1.801164190726142, 2.2598457548241035, 4.0610099455502455, 1.481392287300124, 11.097856380126291, 0.5630968795023756, 2.7576830621987205, -1.4450333828289441, 1.3443471858318305, 0.5584354814932501, -3.1231366661141697, 4.7844589165665425, 1.8626946444466004, -4.963456600116967, -0.9360087202324258, 3.2890824352390418, 2.808026160697277, -7.883356313033259, 1.6417443788140456, 5.73165499202087, -1.890663032501354, -1.2445932684365422, -3.139917698947022, -3.458757322771215, -2.187127945881744, -2.7576830621987205, -2.629028477146853, -2.9739719298221496, 9.173631281959233, -2.0379632095897238, -2.5432587537789417, 7.4806115250448055, 2.1684823538452416, 2.500373892094986, -5.108892218001686, -3.826075485890314, -9.181089518773833, -5.127537810038189, -5.332639322439716, 4.411547075836493, 6.727329606770104, 0.8138800923933344, 2.187127945881744, -2.4947802144840354, -0.6460697640648118, 2.5507169905935427, -0.9150324291913604, -1.8291325787808959, -2.395958576690572, 1.4030808007468136, 2.5413941945752914, -1.480460007698299, 5.6570726238748605, -2.886337647250588, 8.659012941751763, 1.7368368982002085, -0.3964519006761345, -1.695816595719903, 1.9820264334802162, -3.468080118789466, 5.392305216956524, -6.745975198806606, -8.644096468122562, 0.6609862376940138, 6.630372528180291, -2.4798637408548334, -3.587411907823082, 3.4624864411785152, 2.724120996533016, -2.0081302623313197, -4.24373674750797, -0.3244333014351436, -2.793109687068075, 0.4861838123518028, 11.731806509367376, -1.370451014682934, -0.06222966342182711, -6.525957212775877, -6.958534948022735, -0.3004271016881466, -1.9745681966656152, 2.4425725567818284, -6.9100564087278284, 3.212635507889382, -1.6725096056742748, 3.110084751688618, 3.0093985546915043, -1.7396337370056838, 2.5880081746665478, 2.863962936806785, -3.305863468071894, 11.61247472033376, 3.4848611516223182, -3.893199617221723, 2.0640670384408275, 5.698092926355166, -4.251194984322571, -2.5936018522774984, 2.8322654303447306, -3.0485542979681597, 1.6594576912487229, 3.341290092941249, -1.4925796425220255, -5.1909328229622975, -1.8953244305104797, 2.856504699992184, 0.21232667931567228, -0.9863518187309825, -0.2631359176151416, -6.02625534619761, 9.680791385352101, -4.217632918656867, 2.746495706976819, 3.4848611516223182, 0.9057096331731092, -0.8581633734800278, 1.6622545300541984, -3.428924375512811, -2.718527318922065, 1.7946382335133662, 7.995229865252274, 3.132459462132421, 4.445109141502197, -2.5246131617424394, 2.968378252211199, 5.045497205077578, 2.834129989548381, -6.786995501286912, 5.142454283667391, 3.095168278059416, -2.640215832368755, -0.982622700323682, -3.5016421844551706, -6.3208557003743495, -3.4196015794945596, 1.333159830609929, -9.188547755588434, -7.353821499196588, 2.629028477146853, 0.36195755540860486, 0.7197198526089967, -2.878879410435987, 2.284085024471557, 3.05228341637546, -2.5320713985570404, 4.0610099455502455, 9.419753096841065, 1.6035209151392154, 0.792903801352269, 2.174076031456192, -5.970318570088102, 2.9758364890258, -5.023122494633775, -1.3872320475157864, 0.1587206022107276, -0.7677322521029907, -1.1019544893572981, 2.6681842204235084, -7.0144717241322425, -3.1511050541689234, 1.369518735081109, 3.5389333685281756, 3.5613080789719787, -2.1311911697722365, -4.594273877794217, 2.947868100971046, 0.6190336556118832, -1.8766788384739772, -6.432729252593364, 6.048630056641413, -0.1421726392783316, 3.5146940988807223, 3.908116090850925, 1.7685344046622626, 3.4941839476405696, -2.2784913468606063, 0.6003880635753807, 0.8395177814435253, 6.499853383924774, 3.3897686322361555, 0.9574511510744036, -4.01998964306994, -4.903790705600159, -5.526553479619342, -0.44096825166328424, 0.7434929824555374, 1.4422365440234688, 2.2039089787145962, 0.3531008991912662, -4.098301129623251, 3.7533576769479544, -3.967781985367733, 4.6949600747913305, 3.12500122531782, -1.7424305758111591, -2.9814301666367506, 3.440111730734712, -3.0466897387645093, -0.7127277555953083, -2.822942634326479, -2.880743969639637, -4.501045917611704, 0.29040509596852654, 8.576972336791153, 1.5093606753548778, -3.1231366661141697, -8.867843572560592, 11.082939906497089, -2.3251053269518622, -0.18797087471799087, -4.005073169440738, 2.793109687068075, -6.540873686405079, -2.250522958805852, 1.5802139250935874, 2.1833988274744436, 1.5727556882789864, 2.151701321012389, -3.279759639220791, 0.3994818093820662, -6.17914920089693, 3.2816241984244408, -2.1032227817174824, 2.9758364890258, 4.415276194243793, -2.4668118264292813, -0.4852515327499777, -1.9857555518875167, 1.5447873002242325, 0.9649093878890046, -0.6120415585981948, 3.328238178515697, -2.017453058349571, 5.821153833796082, -5.73165499202087, 0.3640551845127114, -3.080251804430214, 3.8298046042976144, 4.310860878839379, 3.1921253566492287, 2.2300128075656995, -1.4674080932727471, -1.1709431798923573, -1.8580332464374747, 1.93541245338896, -1.6035209151392154, -1.7377691778020334, -5.664530860689461, 2.0342340911824235, 5.619781439801855, 2.640215832368755, -8.659012941751763, -7.491798880266707, -1.608182313148341, 5.440783756251431, -0.043001396634183904, 6.440187489407966, 1.2091666435671875, 1.7312432205892576, 4.6651271275329265, 0.763070854093865, -7.875898076218658, -1.048814552053266, 3.205177271074781, -3.766409591373506, 6.932431119171632, -17.70585419786278, -3.4643510003821656, -3.9565946301458315, 4.5271497464628085, -7.003284368910341, 3.770138709780807, 0.41719512181674356, 2.366125629432168, -0.6978112819661063, -3.542662486935476, -1.2100989231690127, -8.9275094670774, -1.6324215827957944, 6.749704317213907, 3.4792674740113676, 1.8002319111243168, -7.0443046713906465, 2.918035153712642, 3.2741659616098397, -3.900657854036324, -2.97770104822945, -0.46730515041484405, -1.7452274146166344, 0.5458497068686109, -5.422138164214928, 0.2608052186105788, -5.761487939279274, 5.440783756251431, -0.6679783347077023, 0.34424424297392753, -8.077270470212886, -1.309852840564301, -2.4612181488183307, -0.3353875867565888, -3.169750646205426, 4.049822590328344, 2.3996876950978723, 1.7452274146166344, -6.358146884447354, 0.7654015530984278, -1.3117173997679512, -2.849046463177583, -4.609190351423419, -4.202716445027665, 6.511040739146675, 7.726733339926638, 2.8509110223812333, -0.5761487939279274, -3.387904073032505, 2.4668118264292813, -1.3797738107011854, -0.770529090908466, 1.1485684694485543, -3.8447210779268164, -2.4724055040402324, -5.396034335363825, 3.9416781565166294, -6.044900938234112, 2.3176470901372612, 2.4668118264292813, -6.458833081444467, -0.3682504427209245, 3.134324021336071, -4.135592313696256, -7.197198526089967, 3.8092944530574617, -3.6209739734887867, -3.863366669963319, -1.570891129075336, -0.2430919061759014, 4.657668890718326, 2.090170867291931, -7.778940997628845, -2.097629104106532, 1.063731025682468, -8.25626815376331, -3.0410960611535587, 2.90311868008344, 4.0460934719210435, 5.347555796068918, 2.1330557289758865, 1.712597628552755, -8.86038533574599, -0.3458757322771215, 0.2519485623932401, 0.39155743276655264, 2.9348161865454943, -1.1961147291416356, 9.464502517728672, 0.08676027044485073, 1.680900122090701, -11.806388877513387, -4.955998363302366, 3.617244855081486, -1.7955705131151913, -3.6209739734887867, -0.7490866600664882, 0.9984714535547091, -5.761487939279274, 0.44935876807971037, 2.9721073706184993, -1.458085297254496, 3.8223463674830134, -2.68123613484906, 2.425791523948976, -2.069660716051778, -1.1541621470595052, -2.3120534125263106, 0.33375609745339485, 0.6456036242638993, -0.024195569041117703, -2.60665376670305, 7.432132985749899, -0.9052434933721967, 4.157967024140059, -0.9430008172461142, -6.227627740191837, -3.6079220590632346, 0.6260257526255716, 8.55459762634735, 5.026851613041075, 0.9770290227127313, -2.366125629432168, -0.5733519551224521, 5.034309849855677, 1.913037742945157, 2.0006720255167187, 2.134920288179537, -4.571899167350415, -7.204656762904568, 0.48105627454176464, 8.517306442274345, -6.097108595936319, 4.333235589283182, 1.4599498564581461, -8.614263520864158, 2.90311868008344]
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