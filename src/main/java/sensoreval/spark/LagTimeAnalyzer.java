package sensoreval.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import preprocess.spark.ConfigRead;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class LagTimeAnalyzer implements Serializable {

    private static String hdfsPath;
    private static String dataPath;
    private static String outputPath;

    private static int numPart;
    private static Random randObj = new Random();
    private static long maxLagTime = 60393600000l;// 31 Dec 2014 - 28 Feb 2013

    private static ConfigRead configRead;

    public static void loadConfig() throws IOException {
        configRead = new ConfigRead();
    }

    public static void main(String args[]) throws Exception {
        loadConfig();
        numPart = configRead.getNumPart();
        hdfsPath = configRead.getHdfsPath();
        dataPath = hdfsPath + configRead.getDataPath();
        outputPath = hdfsPath + configRead.getOutputPath();
        SparkConf sparkConfig = new SparkConf().setAppName("LagTime");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
        SQLContext sqlContext = new SQLContext(sparkContext);
        sqlContext.sql("SET spark.sql.shuffle.partitions=" + numPart);

        sqlContext.read().parquet(dataPath + "user_hashtag_birthday_parquet").coalesce(numPart).registerTempTable("user_hashtag_birthday");
        DataFrame user_mention_grouped = sqlContext.read().parquet(dataPath + "user_mention_grouped_parquet").coalesce(numPart);
        DataFrame tweet_user = sqlContext.read().parquet(dataPath + "tweet_user_parquet").coalesce(numPart);
        sqlContext.read().parquet(dataPath + "tweet_hashtag_time_parquet").coalesce(numPart).registerTempTable("tweet_hashtag_time");

        /************** prepare needed tables ***********/
        //long firstBday = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy").parse("Thu Feb 28 00:00:00 +0000 2013").getTime();

        //todo: change desired set of hashtags here
        //sqlContext.sql("select hashtag, min(uhb.birthday) AS birthday from user_hashtag_birthday uhb" +
        //        " GROUP BY hashtag HAVING (COUNT(username) BETWEEN 2500 AND 500000) AND (min(uhb.birthday) > 1362009600000)").
        sqlContext.sql("select hashtag, min(uhb.birthday) AS birthday from user_hashtag_birthday uhb " +
                "WHERE hashtag IN "+getRedaHashtagList()+" GROUP BY hashtag HAVING min(uhb.birthday) > 1362009600000").
                distinct().registerTempTable("hashtag_birthday_nbrUsers");
        sqlContext.cacheTable("hashtag_birthday_nbrUsers");

        sqlContext.sql("select distinct uhb.username AS username, uhb.hashtag, uhb.birthday " +
                "from user_hashtag_birthday uhb, hashtag_birthday_nbrUsers hb where uhb.hashtag = hb.hashtag")
                .registerTempTable("user_hashtag_birthday");
        user_mention_grouped.registerTempTable("user_mention");
        tweet_user.registerTempTable("tweet_user");
        sqlContext.cacheTable("user_hashtag_birthday");
        sqlContext.cacheTable("tweet_user");
        sqlContext.cacheTable("tweet_hashtag_time");
        sqlContext.cacheTable("user_mention");

        long tweetCount = 829026458;//tweet_user.count();//829026458;
        long user_mentioned1 = 67595249;//user_mention_grouped.count();//67595249;
        System.out.println("USER MENTIONED GROUP: " + user_mentioned1);
        // ---------------------- THE LOOP-----------------------------

        int numIter = 50;
        double cgSize = 1000.;
        long time1, time2;

        for (int i = 0; i < numIter; ++i) {

            time1  = System.currentTimeMillis();
            System.err.println("======================================" + i + "=============================================");
            System.err.println(cgSize / user_mentioned1);
            String uCG = getUsersAsString(user_mention_grouped.sample(false, cgSize / user_mentioned1), cgSize, user_mentioned1, user_mention_grouped);

            JavaRDD<String> BRows = sqlContext.sql(
                    "SELECT username AS username, mentionee FROM user_mention WHERE username IN " + uCG
            ).javaRDD().map(
                    new Function<Row, String>() {
                        public String call(Row row) throws Exception {
                            String[] mentionsList = row.get(1).toString().split(",");
                            return mentionsList[randObj.nextInt(mentionsList.length)];
                        }
                    });
            String uSG = getUsersAsString(BRows, uCG, cgSize, sqlContext);

            long N = sqlContext.sql(
                    "SELECT tid FROM tweet_user WHERE username IN " + uCG
            ).count();

            DataFrame B = sqlContext.sql(
                    "SELECT tid FROM tweet_user WHERE username IN " + uSG
            );
            B.cache();
            long M = B.count();

            (tweet_user.sample(false, (double) (M) / tweetCount)).registerTempTable("C");
            (B.sample(false, ((double) N / M)).distinct()).registerTempTable("D");
            B.unpersist();

            System.err.println("=========================START COMPUTING LAG TIME===============================");
            sqlContext.sql(
                    "SELECT uhb.hashtag, min(birthday) AS birthday from user_hashtag_birthday uhb WHERE uhb.username IN "
                            +uCG+" GROUP BY hashtag").registerTempTable("firstUsage");
            sqlContext.sql("select hb.hashtag, f.birthday - hb.birthday as Lag" +
                    " from hashtag_birthday_nbrUsers hb" +
                    " LEFT OUTER JOIN firstUsage f" +
                    " on hb.hashtag = f.hashtag").registerTempTable("LagTable");
            DataFrame partRes = sqlContext.sql("select hashtag, COALESCE(Lag,"+maxLagTime+") as Lag_A from LagTable");
            sqlContext.dropTempTable("LagTable");
            sqlContext.dropTempTable("firstUsage");
            partRes.write().mode(SaveMode.Overwrite).parquet(outputPath + "result_A_" + i + "_csv");

            sqlContext.sql(
                    "SELECT uhb.hashtag, min(birthday) AS birthday from user_hashtag_birthday uhb WHERE uhb.username IN "
                            + uSG + " GROUP BY hashtag").registerTempTable("firstUsage");
            sqlContext.sql("select hb.hashtag, f.birthday - hb.birthday as Lag" +
                    " from hashtag_birthday_nbrUsers hb" +
                    " LEFT OUTER JOIN firstUsage f" +
                    " on hb.hashtag = f.hashtag").registerTempTable("LagTable");
            partRes = sqlContext.sql("select hashtag, COALESCE(Lag,"+maxLagTime+") as Lag_B from LagTable");
            sqlContext.dropTempTable("LagTable");
            sqlContext.dropTempTable("firstUsage");
            partRes.write().mode(SaveMode.Overwrite).parquet(outputPath + "result_B_" + i + "_csv");

            //TODO: NOTE that C & D are tweet based, so, you cannot use user-hashtag-birthday table
            System.out.println("===================BEFORE=================");
            sqlContext.sql("SELECT tht.hashtag, min(tht.time) AS birthday from tweet_hashtag_time tht" +
                    " INNER JOIN C on tht.tid = C.tid group by hashtag").registerTempTable("firstUsage");
            System.out.println("===================AFTER=================");
            sqlContext.sql("select hb.hashtag, f.birthday - hb.birthday as Lag" +
                    " from hashtag_birthday_nbrUsers hb" +
                    " LEFT OUTER JOIN firstUsage f" +
                    " on hb.hashtag = f.hashtag").registerTempTable("LagTable");
            partRes = sqlContext.sql("select hashtag, COALESCE(Lag,"+maxLagTime+") as Lag_C from LagTable");
            sqlContext.dropTempTable("LagTable");
            sqlContext.dropTempTable("firstUsage");
            partRes.write().mode(SaveMode.Overwrite).parquet(outputPath + "result_C_" + i + "_csv");

            sqlContext.sql("SELECT tht.hashtag, min(tht.time) AS birthday from tweet_hashtag_time tht" +
                    " INNER JOIN D on tht.tid = D.tid group by hashtag").registerTempTable("firstUsage");
            sqlContext.sql("select hb.hashtag, f.birthday - hb.birthday as Lag" +
                    " from hashtag_birthday_nbrUsers hb" +
                    " LEFT OUTER JOIN firstUsage f" +
                    " on hb.hashtag = f.hashtag").registerTempTable("LagTable");
            partRes = sqlContext.sql("select hashtag, COALESCE(Lag,"+maxLagTime+") as Lag_D from LagTable");
            sqlContext.dropTempTable("LagTable");
            sqlContext.dropTempTable("firstUsage");
            partRes.write().mode(SaveMode.Overwrite).parquet(outputPath + "result_D_" + i + "_csv");


            System.err.println("=======================================" + i + " Finished: N: " + N + " M: " + M + "=============================");

            //partRes.coalesce(1).write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(outputPath + "result_" + i + "_csv");
            time2 = System.currentTimeMillis() - time1;
            System.err.println("**********************************Time Iteration:" + i + "*************************************: " + String.valueOf(TimeUnit.MILLISECONDS.toMinutes(time2)) + ", " + String.valueOf(TimeUnit.MILLISECONDS.toSeconds(time2) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(time2))));
        }
    }

    private static String getRedaHashtagList() {
        return "('1000daysof1d','100cancionesfavoritas','100happydays','10thingsimattractedto','10thingsyouhatetodo','11million','14daysoffifa','1bigannouncement','1dalbumfour','1dalbumfourfollowspree','1dbigannouncement','1dconcertfilmdvd','1dday','1ddayfollowparty','1ddayfollowspree','1ddaylive','1defervescenciaccfm','1dfireproof','1dfragrance','1dmoviechat','1dmovieff','1dmoviepremiere','1dorlando','1dproposal','1dthisisus','1dthisisusff','1dtoday','1dtuesdaytest','1dwwafilm','1dwwafilmtrailer','2013follow','2013taughtme','2014in5words','2014mama','20cancionesfavoritas','20grandesquemellevodel2013','20personasimportantesenmivida','22personasespeciales','24lad','24seven','25m','3000miles','3daysforexpelledmovie','3daystill5hboss','3yearsofonedirection','40principales1d','42millionbeliebers','44millionbeliebers','44millionbeliebersfollowparty','4musiclfsbeliebers','4musiclfsdirectioners','4musiclfslittlemonsters','4thofjuly','4yearsago5strangersbecame5brothers','4yearsof1d','4yearsof1dfollowspree','4yearsofonedirection','5countries5days','5daysforexpelledmovie','5hcountdowntochristmas','5millionmahomies','5moresecondsofsummer','5sosalbumfollowspree','5sosderpcon','5sosdontstopfollowme','5sosdontstopfollowspree','5sosfambigvote','5sosfamilyfollowspree','5sosgot2millionfollowersfollowparty','5sosupindisstream','5sosvotinghour','5wordsihatetohear','69factsaboutme','6thfan','7millionmahomies','7yearsofkidrauhl','8daysforexpelledmovie','aaandrea','aadian','aaliyahmovie','aaronsfirstcover','aaronto1m','aaronto600k','aaronto700k','aaronto800k','aatwvideo','accela','actonclimate','actonreform','addawordruinamovie','advancedwarfare','aeronow','afazenda','aga3','agentsofshield','ahscoven','ahsfreakshow','albert_stanlie','alcal100k','alcs','alexfromtarget','alfredo1000','aliadosmusical','alitellsall','allday1ddayfollowspree','allieverneed','allonabilla','allthatmattersmusicvideo','allyfollowme','allylovesyou','alsicebucketchallenge','altband','alwayssupportluhan','amas2014','amazoncart','americanproblemsnight','andreganteng','angelatorres','anotherfollowtrain','anthonytuckerca','applausevid819','applelive','applepay','aprilwishes','areyoutheone','arianagrandeptw','arianalovesyou','ariananow','arianatorshelparianators','artistoftheyearhma','artrave','ash5sosfollowme','askarianagrande','askkendallschmidt','askmiley','asknash','asktyleranything','austinto6m','austinto6million','automaticturnons','azadimarchpti','azadisquare','bail4bapuji','bailon7thjan','bamforxmas','bamshiningstar','bangabanga','bangerztour','bap1004','bapuji','batalla_alboran','batimganteng','bb16','bb8','bbathechase','bbcan2','bbhotshots','bblf','bbma','bbmas','bbmzansi','beafanday','believeacousticforgrammy','believemovieposter','believemovieweekend','believepremiere','bellletstaik','bestcollaboration','bestfandom','bestfandom2014','bestplacetolistentobestsongever','bestsongevermusicvideotodayfollowparty','betawards2014','bethanymotacollection','bethanymotagiveaway','bieberchristmas','blacklivesmatter','bluemoontourenchile','bostonstrong','brager','bravsger','brazilvsgermany','breall','brelandsgiveaway','brentrivera','brentto300k','bringbackourgirls','bringbackourmarine','britishband','britsonedirection','buissness','bundyranch','buybooksilentsinners','buyproblemonitunes','bythewaybuytheway','cabletvactress','cabletvdrama','caiimecam','cailmecam','calimecam','callmesteven','cameronmustgo','camfollowme','camilacafollowspree','camilachameleonfollowspree','camilalovesfries','camilasayshi','camsbookclub','camsnewvideo','camsupdatevideo','camto1mill','camto2mil','camto2million','camto4mil','camto4mill','camwebstartca','candiru_v2','caoru','caraquici','carinazampini','cartahto1mil','carterfollowme','carterfollowspree','cartersnewvideo','carterto1mil','carterto1million','carterto300k','cashdash','cashnewvideo','cdm2014','ces2014','cfclive','changedecopine','childhoodconfessionnight','christmasday','christmasrocks','closeupforeversummer','clublacura','codghosts','colorssavemadhubalaeiej','comedicmovieactress','comedictvactor','comedictvactress','cometlanding','comiczeroes','conceptfollowspree','confessyourunpopularopinion','confidentvideo','congrats5sos','connorhit2million','connorto800k','contestentry','copyfollowers','cr4u','crazymofofollowspree','crazymofosfollowparty','crazymofosfollowspree','criaturaemocional','crimea','crimingwhilewhite','csrclassics','daretozlatan','darrenwilson','davidables','dday70','defundobamacare','del40al1directionersdream','demandavote','demiversary','demiworldtour','dhanidiblockricajkt48','dianafollowparty','didntgetaniallfollowfollowparty','directionermix1065','directionersandbeliebersfollowparty','directvcopamundial','disneymarvelaabcd','djkingassassin','dodeontti','dogecoin','donaldsterling','donetsk','dontstop5sos','dontstopmusicvideo','drqadri','drunkfilms','dunntrial','e32014','educadoresconlahabilitante','educatingyorkshire','elipalacios','emaazing','emabigge','emabiggestfan','emabiggestfans','emabiggestfans1d','emabiggestfans1dᅠ','emabiggestfans5sos','emabiggestfansarianagrande','emabiggestfansj','emabiggestfansju','emabiggestfansjustinbieber','encorejkt48missionst7','entrechavistasnosseguimos','epicmobclothing','ericgarner','ericsogard','esimposiblevivirsin','esurancesave30','etsymnt','euromaidan','eurovisionsongcontest2014','eurovisiontve','everybodybuytheway','exabeliebers','exadirectioners','exarushers','expelledmovie','expelledmovietonumberone','experienciaantiplan','facetimemecam','factsonly','fairtrial4bapuji','fake10factsaboutme','fakecases','fallontonight','fanarmy','fandommemories2013','farewellcaptain','fatalfinale','faze5','fergusondecision','fetusonedirectionday','ffmebellathorne','fictionalcharactersiwanttomarry','fictionaldeathsiwillnevergetover','fifa15','filmfridays','finallya5sosalbum','finalride','findalice','findingcarter','folllowmecam','follobackinstantly','follow4followed','followcarter','followella','followerscentral','followeverythinglibra','followliltwist','followmeaustincarlile','followmebefore2014','followmebrent','followmecarter','followmecon','followmeconnor','followmehayes','followmejack','followmejg','followmejoshujworld','followmelittlemixstudio','followmenash','followmeshawn','followmetaylor','followpyramid','followtrick','fordrinkersonly','fourhangout','fourtakeover','freebiomass','freejustina','freenabilla','freethe7','funnyonedirectionmemories','funwithhashtag','fwenvivoawards','gabesingin','gamergate','geminisweare','georgeujworld','gerarg','gervsarg','getbossonitunes','getcamto2mil','getcamto2million','getcamto3mil','getcamto3mill','getcamto800k','getcovered','getsomethingbignov7','gha','gigatowndun','gigatowndunedin','gigatowngis','gigatownnsn','gigatowntim','gigatownwanaka','givebackphilippines','gleealong','globalartisthma','gmff','gobetter','gobiernodecalle','goharshahi','gonawazgo','goodbye2013victoria','got7','got7comeback','gotcaketour2014','governmentshutdown','gpawteefey','gpettoe_is_a_scammer','grandtheftautomemories','greenwall','gtaonline','h1ddeninspotify','h1ddeninspotifydvd','haiyan','handmadehour','happybirthdaybeliebers','happybirthdaylouis','happybirthdayniall','happybirthdaytheo','happyvalentines1dfamily','harryappreciationday','hastasiemprecerati','havesandhavenots','hayesnewvideo','hayesto1m','hearthstone','heat5sos','heatjustinbieber','heatonedirection','heatplayoffs','heforshe','hermososeria','hey5sos','hiari','hicam','himymfinale','hiphopawards','hiphopsoty','hollywoodmusicawards','hometomama','hoowboowfollowtrain','hormonesseason2','houston_0998','hr15','hrderby','htgawm','iartg','icc4israel','icebucketchallenge','ifbgaintrain','igetannoyedwhenpeople','iheartawards','iheartmahone','illridewithyou','imeasilyannoyedby','immigrationaction','imniceuntil','imsotiredof','imsousedtohearing','imthattypeofpersonwho','imtiredofhearing','incomingfreshmanadvice','indiannews','inners','intense5sosfamattack','inthisgenerationpeople','ios8','iphone6plus','irememberigotintroublefor','isabellacastilloporsolista','isacastilloporartista','isil','italianmtvawards','itzbjj','iwanttix','iwishaug31st','jackto500k','jalenmcmillan','james900k','jamesfollow','jamesto1m','jamesyammounito1million','janoskianstour','jcfollowparty','jcto1million','jcto700k','jerhomies','jjujworld','joshujworld','justiceformikebrown','justinfollowalljustins','justinformmva','justinmeetanita','justwaitonit','kasabi','kaththi','kcaargentina','kcaᅠ','kcaméxico','kcamexico','kedsredtour','kellyfile','kikifollowspree','kingbach','kingofthrones','kingyammouni','knowthetruth','kobane','kykaalviturungunung','laborday','lacuriosidad','lastnightpreorder','latemperatura','leeroyhmmfollowparty','lesanges6','letr2jil','lhhatlreunion','lhhhollywood','liamappreciationday','libcrib','liesivetoldmyparents','lifewouldbealotbetterif','lindoseria','linesthatmustbeshouted','littlemixsundayspree','livesos','lollydance','longlivelongmire','lopopular2014','lorde','losdelsonido','louisappreciationday','lovemarriottrewards','mabf','macbarbie07giveaway','madeinaus','madisonfollowme','magconfollowparty','mahomiesgohardest','makedclisten','malaysiaairlines','maleartist','mama2013','mandelamemorial','mariobautista','marsocial','martinastoessel','maryamrajavi','mattto1mil','mattto1mill','mattto2mill','mattyfollowspree','mchg','meetthevamily','meetthevamps','meninisttwitter','mentionadislike','mentionpeopleyoureallylove','mentionsomeoneimportantforyou','mercedeslambre','merebearsbacktoschool','metrominitv','mgwv','mh17','mh370','michaelbrown','michaelisthebestest','midnightmemories','midnightmemoriesfollowparty','mileyformmva','mipreguntaes','miprimertweet','mis10confesiones','mis15debilidades','missfrance2014','missfrance2015','mixfmbrasil','mmlp2','mmva','monstermmorpg','monumentour','moremota','motafam','motatakeover','movieactress','movimentocountry','mplaces','mpointsholiday','mrpoints','mtvclash','mtvh','mtvho','mtvhot','mtvhott','mtvhotte','mtvhottes','mtvkickoff','mtvmovieawards','mtvs','mtvst','mtvsta','mtvstar','mtvsummerclash','mufclive','mufflerman','multiplayercomtr','murraryftw','murrayftw','musicjournals','my15favoritessongs','myboyfriendnotallowedto','myfourquestion','mygirlfriendnotallowedto','mynameiscamila','myxmusicawards','my_team_pxp','nabillainnocente','nairobisc','nakedandafraid','nash2tomil','nashsnewvid','nashsnewvideo','nashto1mill','nashto2mil','nblnabilavoto','neonlightstour','networktvcomedy','net_one','neversurrenderteam','newbethvideo','newsatquestions','newyearrocks','nhl15bergeron','nhl15duchene','nhl15oshie','nhl15subban','niallappreciationday','niallsnotes','nightchanges','nightchangesvideo','nionfriends','nj2as','no2rouhani','nominateaustinmahone','nominatecheryl','nominatefifthharmony','nominatethevamps','nonfollowback','noragrets','notaboyband','notersholiday2013','nothumanworld','noticemeboris','notyourshield','nowmillion','nowplayingjalenmcmillanuntil','nudimensionatami','nwts','o2lfollowparty','o2lhitamillion','occupygezi','officialsite','officialtfbjp','oitnb','onedirectionencocacolafm','onedirectionformmva','onedirectionptw','onedirectionradioparty','onemoredaysweeps','onenationoneteam','onsefollowledimanchesanspression','operationmakebiebersmile','oppositeworlds','opticgrind','orangeisthenewblack','orianasabatini','oscartrial','othertech','pablomartinez','pakvotes','parentsfavoriteline','paulachaves','paulafernandes','pcaforsupernatural','pdx911','peachesfollowtrain','pechinoexpress','peligrosincodificar','peopieschoice','peopleireallywanttomeet','peoplewhomademyyeargood','perduecrew','perfectonitunes','perfectoseria','peshawarattack','peterpanlive','playfreeway','pleasefollowmecarter','popefrancis','postureochicas','pradhanmantri','praisefox','prayforboston','prayformh370','prayforsouthkorea','prayforthephilippines','praytoendabortion','preordershawnep','pricelesssurprises','queronotvz','qz8501','randbartist','rapgod','raplikelilwayne','rbcames','rcl1milliongiveaway','rdmas','re2pect','realliampaynefollowparty','redbandsociety','relationshipgoals','rememberingcory','renewui','renhotels','replacemovietitleswithpope','retotelehit','retotelehitfinalonedirection','retweetback','retweetfollowtrain','retweetsf','retweetsfo','retweetsfollowtrain','rickychat','ripcorymonteith','riplarryshippers','ripnelsonmandela','rippaulwalker','riprobinwilliams','riptalia','ritz2','rmlive','rollersmusicawards','sammywilk','sammywilkfollowspree','samwilkinson','savedallas','sbelomusic','sbseurovision','scandai','scandalfinale','scarystoriesin5words','scifiactor','scifiactress','scifitv','selenaformmva','selenaneolaunch','selffact','setting4success','sexylist2014','sh0wcase','shareacokewithcam','sharknado','sharknado2','sharknado2thesecondone','shawnfollowme','shawnsfirstsingle','shawnto1mil','shawnto500k','shelookssoperfect','sherlocklives','shotsto600k','shotties','shouldntcomeback','simikepo','simplementetini','skipto1mill','skywire','smoovefollowtrain','smpn12yknilaiuntertinggi','smpn12yksuksesun','smurfsvillage','smurfvillage','sobatindonesia','socialreup','somebodytobrad','somethingbigatmidnight','somethingbigishappening','somethingbigishappeningnov10','somethingbigtonumber1','somethingbigvideo','somethingthatwerenot','sometimesiwishthat','sonic1d','sosvenezuela','soydirectioner','spamansel','spinnrtaylorswift','sportupdate','spreadcam','spreadingholidaycheerwithmere','ss8','ssnhq','standwithrand','standwithwendy','starcrossed','staystrongexo','stealmygiri','stealmygirl','stealmygirlvevorecord','stealmygirlvideo','stilababe09giveaway','storm4arturo','storyofmylife16secclip','storyofmylifefollowparty','storyofmylifefollowspree','summerbreaktour','superjuniorthelastmanstanding','supernaturai','sydneysiege','takeoffjustlogo','talklikeyourmom','talktomematt','tampannbertanya','taylorto1m','taylorto1mill','taylorto58285k','taylorto900k','tayto1','tcas2014','tcfollowtrain','teamfairyrose','teamfollowparty','teamfree','teamgai','teamrude','techtongue','teenawardsbrasil','teenchoice','telethon7','tfb_cats','thanksfor3fantasticyears1d','thankyou1d','thankyou1dfor','thankyoujesusfor','thankyouonedirectionfor','thankyousachin','thankyousiralex','that1dfridayfeelingfollowspree','thatpower','the100','thebiggestlies','theconjuring','thefosterschat','thegainsystem','thegifted','thehashtagslingingslasher','themonster','themostannoyingthingsever','thepointlessbook','thereisadifferencebetween','thesecretonitunes','thevamps2014','thevampsatmidnight','thevampsfollowspree','thevampssaythanks','thevampswildheart','theyretheone','thingsisayinschoolthemost','thingsiwillteachmychild','thingspeopledothatpissmeoff','thisisusfollowparty','thisisusfollowspree','threewordsshewantstohear','threewordstoliveby','tiannafollowtrain','time100','timepoy','tinhouse','tipsfornewdirectioners','tipsforyear7s','titanfall','tityfolllowtrain','tixwish','tns7','tntweeters','tokiohotelfollowspree','tomyfuturepartner','tonyfollowtrain','topfiveunsigned','topfollow','topfollowback','topretw','topretweet','topretweetgaintra','topretweetgaintrai','topretweetgaintrain','topretweetmax','toptr3ce','torturereport','totaldivas','toydefense2','tracerequest','trevormoranto500k','trndnl','truedetective','ts1989','tvbromance','tvcrimedrama','tvgalpals','tvtag','tweeliketheoppositegender','tweetlikejadensmith','tweetliketheoppositegender','tweetmecam','tweetsinchan','twitterfuckedupfollowparty','twitterpurge','uclfinal','ufc168','ultralive','unionjfollowmespree','unionjjoshfollowspree','unionjustthebeginning','unitedxxvi','unpopularopinionnight','uptime24','usaheadlines','user_indonesia','utexaspinkparty','vampsatmidnight','verifyaaroncarpenter','verifyhayesgrier','vevorecord','vincicartel','vinfollowtrain','visitthevamps','vmas2013','voiceresults','voicesave','voicesaveryan','vote4austin','vote4none','vote5hvma','vote5hvmas','vote5os','vote5sosvmas','voteaugust','votebilbo','voteblue','votecherlloyd','votedemilovato','votedeniserocha','votefreddie','votegrich','votejakemiller','votejennette','votejup','votekatniss','votekluber','voteloki','voteluan','votematttca','votemiley','votemorneau','voterizzo','votesamandcat','votesnowwhite','votesuperfruit','votetimberlake','votetris','votetroyesivan','voteukarianators','voteukdirectioners','voteukmahomies','votevampsvevo','voteveronicamars','votezendaya','waliansupit','weakfor','weareacmilan','weareallharry','weareallliam','weareallniall','weareallzayn','wearewinter','webeliveinyoukris','wecantbeinarelationshipif','wecantstop','welcometomyschoolwhere','weloveyouliam','wethenorth','wewantotrainitaly2015','wewantotratourinitaly2015','wewillalwayssupportyoujustin','whatidowheniamalone','wheredobrokenheartsgo','whereismike','wherewearetour','whitebballpains','whitepeopleactivities','whowearebook','whybeinarelationshipif','whyimvotingukip','wicenganteng','wildlifemusicvideo','wimbledon2014','win97','wmaexo','wmajustinbieber','wmaonedirection','womaninbiz','wordsonitunestonight','worldcupfinal','worldwara','wwadvdwatchparty','wwat','wwatour','wwatourfrancefollowspree','xboxone','xboxreveal','xf7','xf8','xfactor2014','xfactorfinal','yammouni','yeremiito21','yesallwomen','yespimpmysummerball','yespimpmysummerballkent','yespimpmysummerballteesside','yolandaph','youmakemefeelbetter','younusalgohar','yourstrulyfollowparty','ytma','z100jingleball','z100mendesmadness','zaynappreciationday','zimmermantrial','__tfbjp_')";
    }

    public static String getUsersAsString(DataFrame users, double cgSize, long user_mentioned1, DataFrame user_mention_grouped) {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        int ind = 0;
        for (Row r: users.distinct().collect()) {
            sb.append('\'').append(r.get(0).toString()).append("\',");
            ind++;
            if(ind == cgSize)
                break;
        }
        // Sample again if the number of distinct users where not exactly 1000
        if(ind < cgSize){
            users = user_mention_grouped.sample(false, (double) cgSize / user_mentioned1);
            for (Row r: users.distinct().collect()) { //might cause OOM on driver !!!
                sb.append('\'').append(r.get(0).toString()).append("\',");
                ind++;
                if(ind == cgSize)
                    break;
            }
        }
        int lastIdx = sb.length() - 1;
        if (sb.charAt(lastIdx) == ',') {
            sb.deleteCharAt(lastIdx);
        }
        sb.append(')');
        return sb.toString();
    }

    public static String getUsersAsString(JavaRDD<String> users, String uCG, double cgSize, SQLContext sqlContext) {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        int ind = 0;
        for (String s: users.distinct().collect()) { //might cause OOM on driver !!!
            sb.append('\'').append(s).append("\',");
            ind++;
        }

        // Sample again if the number of distinct users where not exactly 1000
        if(ind < cgSize){
            System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX Need to sample again-size: "+ind+" XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
            users = sqlContext.sql(
                    "SELECT username, mentionee FROM user_mention WHERE username IN " + uCG
            ).javaRDD().map(
                    new Function<Row, String>() {
                        public String call(Row row) throws Exception {
                            String[] mentionsList = row.get(1).toString().split(",");
                            return mentionsList[randObj.nextInt(mentionsList.length)];
                        }
                    });
            for (String s: users.distinct().collect()) { //might cause OOM on driver !!!
                sb.append('\'').append(s).append("\',");
                ind++;
                if(ind == cgSize)
                    break;
            }
        }
        System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX SG SIZE: "+ ind + "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
        int lastIdx = sb.length() - 1;
        if (sb.charAt(lastIdx) == ',') {
            sb.deleteCharAt(lastIdx);
        }
        sb.append(')');

        return sb.toString();
    }

}