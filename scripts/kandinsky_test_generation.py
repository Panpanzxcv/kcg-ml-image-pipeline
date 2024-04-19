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
    image_embedding_list = [-4.863702682721678, 1.4543561788471955, -5.705551163169766, -2.56470118462092, 1.756414769838536, 2.5688964428291325, -1.3417834169268115, 1.3557676109541883, -6.220169503377235, -2.3381572413774143, -2.4304529219581017, -5.610458643783604, 1.3243031743925906, 1.1005560699545605, 3.031773265135307, 2.9646491338038983, -1.4012162415431633, 0.8614263520864157, 2.384305081667758, 0.09692794485225599, -2.2039089787145962, 0.7530488483742449, -3.241536175545961, -1.9829587130820414, 1.8123515459480435, -0.21902743895379037, 2.9422744233600953, -3.227551981518584, -4.31831911565398, 0.6527122562278158, -1.6725096056742745, -6.662070034642346, -2.468210245832019, -3.4848611516223187, 2.785651450253474, 0.2206006607818703, 1.7144621877564055, -0.3216364626296682, 0.9040781438699153, 4.715470226031483, 2.875150292028686, 0.8705160782042107, -1.3124166094693202, -0.6733389424181968, 2.2164947533392354, 1.0809781983162328, -7.80877394488725, -3.6498746411453658, -0.32810415236733004, -3.0933037188557657, 1.2921395281296235, -4.642752417089124, -3.205177271074781, -1.913037742945157, 0.4621776126048059, 2.212299495131022, -4.533675703675584, -1.0138540669848237, 0.6418745058565987, 1.5746202474826365, -0.8754105461137927, -0.11799163710599242, 11.780285048662282, -1.5508471176360958, 0.8893947401411695, 0.2775862514434311, 1.164883362480494, -1.2977332057405744, -2.7367067711576554, 1.0809781983162328, 2.75208938458777, 2.1353864279804493, 4.0610099455502455, 0.07376662349441304, -2.719925738324803, 0.971901484902693, 4.259585500738997, 0.7845132849358429, 2.9031186800834403, -6.102702273547271, -4.432989506678471, -0.8614263520864157, -2.7171288995193272, -0.2718177714071381, -1.2208201385900015, 2.9226965517217676, 7.394841801676894, 4.458161055927749, 1.6165728295647672, 3.174412044214552, -0.36533706896522095, 0.5964258752676239, -4.530878864870109, 1.7634068668522245, -2.7926435472671627, 0.11362157647243715, -1.6011902161346527, 2.4346481801663145, -4.410614796234667, -3.9127774888600504, -2.908712357694391, -2.6010600890920994, -1.0669940042888557, -4.024651041079066, 5.974047688495403, -0.1912338533243788, -2.4192655667362, 0.7761227685194168, -5.305603213986788, 2.0500828444134505, -2.8164166771137036, -3.347816050154025, 2.132589589174974, -3.38697179343068, -0.7121450808441676, -1.478129308693736, 5.048294043883053, 0.8921915789466448, 0.7998958983659574, -0.8404500610453505, 1.492113502721113, -2.9003218412779646, -1.4739340504855232, 2.5940679920784113, -2.6010600890920994, -0.19472990183122305, -0.27968388054753757, 3.448502247151138, 3.554782121759202, 0.7425607028537122, 0.20958810798531097, -3.2471298531569115, 1.016650905790299, -0.7432599125550811, -3.5016421844551706, -4.5420662200920106, 0.05716039308690299, 0.6383784573497545, -1.8123515459480435, -3.879215423194346, 8.659012941751763, 6.852255073414671, -1.016650905790299, -0.9117694505849725, -2.4668118264292813, -4.4525673783167985, 5.624442837810981, 3.420533859096384, -1.0334319386231514, 2.124199072758548, -1.9633808414437137, -1.8123515459480435, -0.6020195528785747, 7.618588906114924, -2.872353453223211, 1.626361765383931, -1.0432208744423153, -2.3003999175034964, 4.086181494799524, -1.28235059231046, -0.0723245034853398, -0.2508414803660728, 2.3003999175034964, -3.045757459162684, 2.630426896549591, -3.0038048770805537, 1.200543057250305, -2.696152608478262, -2.8220103547246542, 0.08307485264388577, -1.2194217191872638, -0.12638215352241855, -0.1370451014682934, 1.1096457960723554, -3.227551981518584, -1.3410842072254427, 3.2750982412116656, -2.3059935951144475, 1.9717713578601397, -0.7481543804646631, -2.840189806960244, 4.198055047018539, 0.06410878949425589, -4.416208473845618, -0.9537220326671032, 0.31918922867487726, 2.45562447120738, -1.0481153423518972, -0.7313733476318107, 2.3395556607801518, -2.710136802505639, 1.2292106550064277, 3.7169987724767743, 4.80217222900122, -0.5806936569868248, -0.8075872050810149, -3.4568927635675646, -1.6235649265784557, -3.252723530767862, 8.435265837313734, -0.7635369938947776, 0.40169597343640084, -11.00835753835108, -1.3466778848363936, -0.878207384919268, 2.079449651870942, -0.541887518560854, -2.835994548752031, 16.344725979198095, -1.1578912654668057, -2.9506649397765212, 2.6192395413276897, 0.09282008785671403, -2.603856927897575, 2.56470118462092, -0.5771976084799807, -2.431851341360839, -3.389768632236155, -0.9117694505849725, 0.17375361079015772, 3.1128815904940934, -1.2655695594776075, -6.841067718192768, 2.0444891668024994, 2.469608665234757, -1.7200558653673563, -0.855133464774096, 0.4883979764061375, 7.171094697238864, -3.563172638175629, 7.652150971780628, 0.39435427157202796, -0.4317619905952611, 0.994276195346496, -0.3543245161686617, 2.8779471308341615, -1.362060498266508, 4.841327972277876, 1.3208071258857461, -2.115808556342122, -0.658655538689451, -3.2359424979350098, -4.377052730568963, 1.9200298399588456, 3.2331456591295344, -1.4061107094527452, -2.3885003398759705, 0.607263625638841, 3.504439023260646, 4.5896124797850915, 0.37652442418712245, 8.513577323867043, -6.4886660287028715, 3.781326065002708, -2.9366807457491446, -4.749032291697188, -3.915574327665526, 1.0320335192204138, -4.905655264803809, 0.4635760320075436, 0.26762251319892505, -4.860905843916203, -3.884809100805297, -0.42721712753636365, -6.18101376010058, -4.852515327499777, -5.694363807947865, 0.9516244035629966, 3.468080118789466, -2.476600762248445, -7.254999861403125, 0.6320855700374349, -1.021545373699881, 1.9913492294984676, -2.6444110905769675, -1.701876413131766, -4.525285187259158, 5.28043166473751, 2.9506649397765212, 3.9407458769148045, -4.846921649888826, 3.0877100412448146, -1.4131028064664337, 2.280822045865169, 4.639955578283649, -1.1439070714394288, 3.691827223227496, 0.8166769311988097, 3.840059679917691, 5.227291727433477, 1.1320205065161584, -11.858596535215593, -3.856840712750543, -3.3813781158197296, -0.6544602804812379, -2.742300448768606, 1.3823375796062045, 0.15941981191209642, -2.9590554561929476, 1.3557676109541883, 2.1353864279804493, 0.09002324905123865, 0.873312917009686, 3.104491074077667, 3.7114050948658237, -2.0067318429285823, -1.703274832534504, 1.039724825935471, -3.09050688005029, 1.0991576505518228, -7.534683741950662, -0.07931660049902824, 4.480535766371552, -2.1353864279804493, 5.957266655662551, -3.2135677874912068, -2.6569968652016067, 3.4429085695401875, -1.8543041280301744, 3.0121953934969796, 0.6841766927894138, -5.232885405044429, 0.20329522067299138, 0.20329522067299138, -9.263130123734445, 0.739064654346868, 4.9196394588311865, 0.3744267950830159, 1.7438289952138968, 0.4586815640979616, 0.18633938541479692, 1.8123515459480435, 1.5941981191209642, -0.9040781438699153, -4.480535766371552, 0.2555611458503124, 9.22397438045779, 0.9795927916177505, -7.495527998674007, -0.783814075234474, 13.368889490172295, 0.19717713578601398, -6.208982148155334, 7.417216512120698, 0.18686379269082354, -1.9913492294984676, -3.4568927635675646, 3.359003405375926, 3.5156263784825477, -0.815977721497441, 3.812091291862937, -0.8642231908918911, -5.35874315129082, -0.7586425259851957, -2.4528276324019047, -2.208104236922809, -2.5297406995524776, 2.0053334235258444, 0.3593937865035858, 0.40833846559940484, -2.8290024517383427, -1.0669940042888557, 8.726137073083173, 2.4584213100128554, -3.764545032169856, 2.0402939085942866, -2.9478681009710463, 3.031773265135307, -4.150508787325458, 4.301538082821128, -0.7810172364289987, -1.7186574459646184, -3.028976426329832, 2.8038309024890644, 1.6305570235921443, -5.423070443816753, -5.5377408348412445, -0.6432729252593364, -3.8680280679724444, 6.02998446460491, 5.1014339811870855, 0.8921915789466448, -3.770138709780807, -1.5382613430114565, 1.3145142385734265, 2.2948062398925457, 5.490194575148163, -0.30153418371531393, 6.897004494302276, -0.39225664246792147, -5.330774763236066, 4.936420491664038, -3.258317208378813, 0.4586815640979616, 0.8376532222398752, 1.6277601847866685, -7.182282052460765, 2.4122734697225114, 1.7368368982002085, -1.3417834169268115, -0.8152785117960721, -8.737324428305074, -3.714201933671299, -2.9198997129162922, -1.9955444877066806, -2.7478941263795567, 3.118475268105044, 3.448502247151138, 2.840189806960244, 0.09596653151287383, -2.2192915921447107, 1.2271130259023213, 3.207974109880256, 1.6305570235921443, -2.2472599801994644, -6.158639049656777, 6.248137891431989, -5.680379613920488, -3.1716152054090765, 1.4124035967650648, 6.91937920474608, 0.19455509940588084, 1.2515853654502307, -0.40554162679392947, -7.092783210685553, -1.339685787822705, -0.546781986470436, 1.3865328378144175, -2.093433845898319, 3.0933037188557657, 2.331165144363726, -2.5940679920784113, -0.6240446584716932, 1.2243161870968458, 2.1297927503694987, -3.224755142713108, 0.6125076983991072, 0.36288983501043004, -4.763016485724565, -1.2613743012693943, 3.431721214318286, 1.5019024385402768, -3.602328381452284, 1.1467039102449041, 0.9656085975903735, -2.0990275235092697, 2.917102874110817, 1.5130897937621783, -0.941136258042464, -1.3613612885651392, -1.1767699274037644, -0.5401394943074319, -3.414940181485434, -1.4019154512445322, -4.6343619006726975, 0.022483961959641887, -2.521350183136051, 0.9642101781876358, -2.496178633886773, -1.1481023296476418, 2.034700230983336, -0.9425346774452017, -0.43840448275826516, -1.564831311663473, -2.5339359577606904, 0.6177517711593736, -1.4501609206389823, 4.712673387226008, -6.208982148155334, 2.7562846427959826, 1.8137499653507811, 0.6366304330963324, -2.366125629432168, 3.283488757628091, 4.788188034973843, -3.9071838112491, 1.8179452235589943, 1.4893166639156377, 1.8333278369891088, -5.655208064671211, -1.7228527041728314, -2.2276821085611367, 1.4683403728745723, -1.0075611796725041, -1.1844612341188216, -5.230088566238953, 2.3857035010704957, 0.49259323461435056, 2.6178411219249518, 5.165761273713019, 3.1911930770474037, 1.9605840026382384, 0.10225941882519343, -1.2942371572337303, -2.2053073981173337, -0.2913956430454657, 1.4515593400417202, 3.4121433426799586, 3.8959964560271985, 0.8537350453713585, -2.629028477146853, -3.624703091896087, 1.455754598249933, 3.031773265135307, -0.0936940999834251, -3.3058634680718946, -0.2625532428640009, 3.283488757628091, 1.7731958026713883, 1.3655565467733521, -0.22357230201268785, -0.7845132849358429, 4.005073169440738, -2.3381572413774143, -2.5283422801497397, -0.6527122562278158, 1.3669549661760898, -7.333311347956435, 2.2039089787145962, 0.6331343845894882, 1.2159256706804196, 0.08255044536785913, 2.427656083152626, -3.915574327665526, -4.813359584223122, 1.703274832534504, -0.15653557189394995, -4.690298676782205, -1.8612962250438625, -0.06576941253500689, -4.704282870809582, 2.490584956275822, -6.164232727267729, 5.17135495132397, -0.331425398448832, 0.04811436757544357, 0.015644817068127884, 2.5786853786482964, 2.5115612473168873, 4.192461369407589, 4.967185718524267, 3.328238178515697, 6.606133258532838, -4.357474858930635, -1.4627466952636214, 3.0485542979681597, -0.3242584990098014, -0.9844872595273323, 1.441770404222556, 4.50570731562083, 1.5382613430114565, -0.4978373073746169, -1.5927996997182268, 0.3246081038604858, -0.48560113760066215, -0.1304900105179605, -2.6262316383413777, -1.2459916878392798, 4.365865375347061, -3.809294453057462, 1.6459396370222588, 0.7292757185277042, -4.399427441012766, -2.9226965517217676, -0.9558196617712096, 2.092035426495581, 5.5237566408138665, -3.073725847217438, -4.09457201121595, -0.4719665484239697, -3.392565471041631, -0.9173631281959232, -2.0626686190380896, 0.8089856244837524, -3.135256300937896, -5.355946312485345, 2.8290024517383427, -2.7786593532397856, 3.356206566570451, -1.276756914699509, -2.5716932816346083, 3.568766315786579, 1.738235317602946, 2.3912971786814463, 0.801993527470064, -0.4212738450747285, 1.3774431116966226, -3.1604278501871748, 2.2570489160186282, -2.662590542812558, -4.226023435073293, -1.236901961721485, 0.5670590678101324, 5.646817548254783, 1.796968932517929, 0.295765703679021, 1.2564798333598126, 3.9966826530243122, 5.218901211017052, 1.4138020161678027, 1.083775037121708, 0.6341831991415414, -0.14027894633712432, 5.308400052792264, 3.255520369573337, 6.7291941659737535, 3.9882921366078854, -6.0803275631034674, -0.508675057745834, -2.608052186105788, -0.05982613007337172, -1.2403980102283292, -1.0858726662258147, 1.1390126035298467, 1.859897805641125, 2.6024585084948373, -1.6767048638824877, 3.6862335456165454, -3.0821163636338644, -0.28562716300917274, 2.7828546114479993, 1.6193696683702425, 4.147711948519982, 2.3549382742102667, -3.2471298531569115, -1.7186574459646184, -0.976795952812275, 0.30765226860229133, 0.3291529669193833, 2.4779991816511826, 3.392565471041631, 0.1730544010887889, -0.45308788648701087, 6.203388470544383, 0.1448238093960218, -1.2424956393324358, 0.9145662893904478, -1.0809781983162328, 4.914045781220235, 3.767341870975331, -0.9257536446123493, 6.807505652527064, 3.5827505098139563, -4.298741244015652, -2.539529635371641, 3.359003405375926, -3.7477639993370033, -1.3375881587185985, 4.905655264803809, 1.1599888945709123, 2.093433845898319, -2.7129336413111145, 0.3436615682227868, 1.1208331512942569, 0.869117658801473, 1.4529577594444576, -0.40659044134598277, -0.5139191305061003, -7.243812506181223, -0.17296699987611777, -2.675176317437197, -0.8208721894070228, -0.7530488483742449, -0.9068749826753906, 0.3807196823953355, 0.40973688500214256, 1.1397118132312156, -3.8764185843888708, -0.33317342270225414, 0.07691306715057283, 1.921428259361583, 0.7156411293510118, 6.281699957097695, -2.0850433294818926, 5.311196891597739, -1.5732218280798989, 1.7312432205892576, -1.0110572281793484, -1.1844612341188216, 2.5227486025387886, 3.744967160531528, -0.8320595446289243, -0.29209485274683455, -1.98995081009573, -3.361800244181402, 5.249666437877281, -1.5340660848032437, 0.24926825853799284, 6.801911974916114, 2.4598197294155932, -2.545123312982592, -5.674785936309537, 0.6338335942908571, 0.5925802219100953, 0.15574896097990998, 1.071189262497069, -2.0906370070928433, 0.15531195491655447, 2.012325520539533, -4.114149882854278, 1.2250153967982145, 3.518423217288023, 0.05126081123160337, 0.528602534234846, 0.24367458092704208, 0.7726267200125725, 3.2163646262966825, -1.3096197706638448, -6.164232727267729, 0.280033485398222, -4.326709632070406, 2.540928054774379, 1.7717973832686504, 1.5382613430114565, -3.2750982412116656, 0.9271520640150872, -1.3907280960226307, 1.2648703497762388, 5.325181085625116, 2.417867147333462, 0.7502520095687695, -6.02998446460491, 0.9383394192369887, -1.892061451904092, -2.0095286817340576, -4.491723121593454, 1.9507950668190746, -1.792773674309716, 1.609580732551079, 0.1984007527634095, -4.086181494799524, 0.1495434748802615, -4.4637547335387, -3.767341870975331, 1.276756914699509, 3.6191094142851368, -1.492113502721113, 5.6440207094493084, -0.6869735315948892, 1.331295271406279, 0.5887345685525667, -0.02989121473351808, -0.9823896304232258, 0.6771845957757254, -6.069140207881565, 1.921428259361583, -2.500373892094986, 1.1040521184614045, -0.09727754970294042, 1.7592116086440115, 4.449770539511323, -2.5954664114811488, 0.8075872050810149, -3.6890303844220202, 1.465543534069097, 1.362060498266508, 2.545123312982592, 0.2961153085297054, 3.9435427157202794, 0.9600149199794228, 6.952941270411784, 1.9731697772628776, 0.9984714535547092, -2.84298664576572, 0.9292496931191936, 0.45308788648701087, -3.8820122619998214, 3.817684969473888, -2.8010340636835886, -4.416208473845618, -0.4153305626130933, 2.297603078698021, 3.7925134202246094, 0.5044797995376209, -0.40973688500214256, 2.8248071935301295, -3.7114050948658237, -3.8232786470848383, -1.4047122900500075, 2.3171809503363487, 7.428403867342598, 1.3837359990089422, -1.6683143474660618, -0.4408517167130561, 0.3183152165481662, -4.584018802174141, -0.49084521036092843, 3.607922059063235, 6.002016076550156, -0.22864157234761195, 2.4192655667362, 4.6343619006726975, 0.44679499917469134, 3.372987599403303, 2.1521674608133017, -1.1529967975572237, -0.7128442905455363, 2.7227225771302783, 0.8187745603029164, -0.5439851476649606, -4.010666847051689, 1.5326676654005058, 0.33789308818649383, 0.7460567513605565, -3.031773265135307, 1.353669981850082, 0.2927940624482034, 0.806188785678277, 0.7446583319578188, -7.36687341362214, -0.7984974789632198, 0.9635109684862668, 0.2254951286914522, 1.331295271406279, -5.5377408348412445, 0.16903394530591803, -2.620637960730427, -1.3683533855788277, -2.222088430950186, 3.68343670681107, 2.5786853786482964, -2.83040087114108, -3.563172638175629, 0.7866109140399494, 7.970990595604821, 0.6034179722813123, 4.374255891763488, -2.931087068138194, -1.587206022107276, 3.8232786470848383, -0.13765690995699115, -3.661061996367267, 7.103970565907455, 0.13407346023747582, -3.1604278501871748, 2.3675240488349054, -2.8387913875575066, -3.708608256060349, 2.559107507009969, 2.517154924927838, -0.8949884177521202, -0.20154719641956925, 2.684965253256361, 1.7200558653673563, 1.066294794587487, 4.09457201121595, -2.0584733608298764, -0.855832674475465, -1.9256235175697962, 2.9674459726093736, 5.325181085625116, 0.7593417356865646, 1.7284463817837823, -1.782984738490552, -2.1801358488680553, -7.266187216625026, 1.0683924236915936, -2.1046212011202203, 4.270772855960899, -0.4363068536541586, 2.215096333936498, -1.756414769838536, 1.6599238310496358, 5.5069756079810155, 1.6333538623976194, -0.4646248465595968, 4.4525673783167985, -2.687762092061836, -0.869117658801473, -1.4543561788471955, 2.8220103547246542, 3.1911930770474037, -0.7649354132975152, 1.5536439564415714, 0.971901484902693, -0.38526454545423305, -5.179745467740396, 1.3508731430446064, 1.6948843161180775, 3.045757459162684, 1.111044215475093, 8.32339228509472, 0.4223226596267817, 2.0682622966490403, -1.083775037121708, 1.0082603893738729, 0.4188266111199376, -2.342352499585627, 3.588344187424907, 1.3970209833349503, -3.722592450087725, -0.7020065401743193, 2.4668118264292813, 2.1060196205229578, -5.912517234774944, 1.231308284110534, 4.298741244015652, -1.4179972743760154, -0.9334449513274066, -2.3549382742102667, -2.5940679920784113, -1.640345959411308, -2.0682622966490403, -1.9717713578601397, -2.2304789473666125, 6.880223461469425, -1.5284724071922928, -1.9074440653342064, 5.610458643783604, 1.626361765383931, 1.8752804190712395, -3.8316691635012647, -2.8695566144177356, -6.885817139080375, -3.845653357528642, -3.999479491829787, 3.30866030687737, 5.045497205077577, 0.6104100692950007, 1.640345959411308, -1.8710851608630266, -0.48455232304860885, 1.913037742945157, -0.6862743218935203, -1.371849434085672, -1.796968932517929, 1.0523106005601102, 1.9060456459314685, -1.1103450057737243, 4.242804467906145, -2.164753235437941, 6.494259706313823, 1.3026276736501563, -0.2973389255071009, -1.2718624467899273, 1.4865198251101621, -2.6010600890920994, 4.044228912717394, -5.059481399104955, -6.483072351091922, 0.49573967827051035, 4.972779396135218, -1.859897805641125, -2.6905589308673115, 2.596864830883886, 2.043090747399762, -1.5060976967484898, -3.1828025606309778, -0.2433249760763577, -2.0948322653010565, 0.3646378592638521, 8.798854882025532, -1.0278382610122005, -0.04667224756637033, -4.894467909581907, -5.218901211017052, -0.22532032626610998, -1.4809261474992115, 1.8319294175863714, -5.182542306545871, 2.4094766309170366, -1.254382204255706, 2.3325635637664637, 2.2570489160186282, -1.304725302754263, 1.9410061309999107, 2.1479722026050885, -2.4793976010539205, 8.70935604025032, 2.6136458637167386, -2.9198997129162922, 1.5480502788306207, 4.273569694766374, -3.1883962382419284, -1.945201389208124, 2.124199072758548, -2.2864157234761198, 1.2445932684365422, 2.5059675697059367, -1.119434731891519, -3.893199617221723, -1.42149332288286, 2.142378524994138, 0.1592450094867542, -0.7397638640482369, -0.19735193821135621, -4.519691509648208, 7.260593539014076, -3.16322468899265, 2.0598717802326143, 2.6136458637167386, 0.6792822248798318, -0.6436225301100209, 1.2466908975406488, -2.5716932816346083, -2.0388954891915487, 1.3459786751350247, 5.996422398939206, 2.349344596599316, 3.333831856126648, -1.8934598713068296, 2.2262836891583992, 3.784122903808184, 2.125597492161286, -5.090246625965184, 3.856840712750543, 2.321376208544562, -1.980161874276566, -0.7369670252427616, -2.6262316383413777, -4.740641775280762, -2.56470118462092, 0.9998698729574468, -6.891410816691326, -5.515366124397441, 1.9717713578601397, 0.27146816655645367, 0.5397898894567476, -2.15915955782699, 1.7130637683536678, 2.289212562281595, -1.8990535489177804, 3.045757459162684, 7.064814822630799, 1.2026406863544115, 0.5946778510142018, 1.6305570235921443, -4.477738927566076, 2.23187736676935, -3.767341870975331, -1.04042403563684, 0.11904045165804569, -0.575799189077243, -0.8264658670179736, 2.0011381653176312, -5.260853793099182, -2.3633287906266927, 1.0271390513108318, 2.654200026396132, 2.6709810592289838, -1.5983933773291774, -3.445705408345663, 2.2109010757282843, 0.4642752417089124, -1.4075091288554828, -4.824546939445023, 4.536472542481059, -0.1066294794587487, 2.6360205741605416, 2.931087068138194, 1.326400803496697, 2.620637960730427, -1.7088685101454546, 0.4502910476815355, 0.629638336082644, 4.8748900379435804, 2.5423264741771168, 0.7180883633058027, -3.014992232302455, -3.677843029200119, -4.144915109714507, -0.3307261887474632, 0.5576197368416531, 1.0816774080176015, 1.6529317340359473, 0.2648256743934496, -3.073725847217438, 2.8150182577109657, -2.9758364890258, 3.521220056093498, 2.343750918988365, -1.3068229318583693, -2.236072624977563, 2.5800837980510343, -2.285017304073382, -0.5345458166964813, -2.1172069757448595, -2.1605579772297276, -3.375784438208778, 0.21780382197639492, 6.432729252593365, 1.1320205065161584, -2.342352499585627, -6.650882679420444, 8.312204929872816, -1.7438289952138968, -0.14097815603849315, -3.0038048770805537, 2.0948322653010565, -4.905655264803809, -1.687892219104389, 1.1851604438201906, 1.6375491206058328, 1.1795667662092397, 1.613775990759292, -2.4598197294155932, 0.2996113570365496, -4.6343619006726975, 2.4612181488183307, -1.5774170862881118, 2.23187736676935, 3.3114571456828448, -1.850108869821961, -0.36393864956248323, -1.4893166639156377, 1.1585904751681744, 0.7236820409167535, -0.45903116894864604, 2.496178633886773, -1.5130897937621783, 4.365865375347061, -4.298741244015652, 0.27304138838453357, -2.3101888533226607, 2.872353453223211, 3.2331456591295344, 2.3940940174869216, 1.6725096056742745, -1.1005560699545605, -0.878207384919268, -1.393524934828106, 1.4515593400417202, -1.2026406863544115, -1.303326883351525, -4.248398145517096, 1.5256755683868177, 4.214836079851391, 1.980161874276566, -6.494259706313823, -5.61884916020003, -1.2061367348612557, 4.080587817188573, -0.03225104747563793, 4.830140617055974, 0.9068749826753906, 1.298432415441943, 3.498845345649695, 0.5723031405703988, -5.906923557163994, -0.7866109140399494, 2.4038829533060855, -2.8248071935301295, 5.199323339378724, -13.279390648397086, -2.598263250286624, -2.9674459726093736, 3.3953623098471066, -5.2524632766827555, 2.8276040323356053, 0.3128963413625577, 1.774594222074126, -0.5233584614745798, -2.6569968652016067, -0.9075741923767595, -6.69563210030805, -1.2243161870968458, 5.06227823791043, 2.609450605508526, 1.3501739333432377, -5.2832285035429845, 2.1885263652844813, 2.45562447120738, -2.925493390527243, -2.2332757861720873, -0.35047886281113305, -1.3089205609624759, 0.4093872801514582, -4.066603623161196, 0.1956039139579341, -4.321115954459455, 4.080587817188573, -0.5009837510307767, 0.2581831822304457, -6.057952852659664, -0.9823896304232258, -1.845913611613748, -0.2515406900674416, -2.3773129846540697, 3.0373669427462584, 1.799765771323404, 1.3089205609624759, -4.768610163335516, 0.5740511648238209, -0.9837880498259635, -2.136784847383187, -3.4568927635675646, -3.152037333770749, 4.883280554360006, 5.7950500049449785, 2.138183266785925, -0.4321115954459456, -2.540928054774379, 1.850108869821961, -1.034830358025889, -0.5778968181813495, 0.8614263520864157, -2.883540808445112, -1.8543041280301744, -4.0470257515228685, 2.956258617387472, -4.533675703675584, 1.738235317602946, 1.850108869821961, -4.844124811083351, -0.27618783204069336, 2.3507430160020535, -3.1016942352721917, -5.397898894567476, 2.8569708397930964, -2.71573048011659, -2.8975250024724892, -1.178168346806502, -0.18231892963192606, 3.493251668038744, 1.567628150468948, -5.834205748221634, -1.5732218280798989, 0.7977982692618509, -6.192201115322483, -2.280822045865169, 2.17733901006258, 3.0345701039407826, 4.010666847051689, 1.5997917967319149, 1.2844482214145663, -6.645289001809493, -0.2594067992078411, 0.18896142179493008, 0.2936680745749145, 2.201112139909121, -0.8970860468562267, 7.098376888296504, 0.06507020283363804, 1.2606750915680256, -8.85479165813504, -3.7169987724767743, 2.7129336413111145, -1.3466778848363936, -2.71573048011659, -0.5618149950498661, 0.7488535901660318, -4.321115954459455, 0.33701907605978276, 2.2290805279638746, -1.093563972940872, 2.86675977561226, -2.010927101136795, 1.819343642961732, -1.5522455370388335, -0.8656216102946288, -1.734040059394733, 0.25031707309004614, 0.4842027181979245, -0.018146676780838276, -1.9549903250272875, 5.574099739312424, -0.6789326200291474, 3.118475268105044, -0.7072506129345857, -4.670720805143878, -2.705941544297426, 0.4695193144691787, 6.415948219760512, 3.770138709780807, 0.7327717670345485, -1.774594222074126, -0.4300139663418391, 3.7757323873917574, 1.4347783072088678, 1.500504019137539, 1.6011902161346527, -3.428924375512811, -5.4034925721784255, 0.3607922059063235, 6.387979831705758, -4.5728314469522395, 3.249926691962387, 1.0949623923436096, -6.460697640648119, 2.17733901006258]
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