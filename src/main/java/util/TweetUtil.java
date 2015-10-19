package util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by zahraiman on 9/29/15.
 */
public class TweetUtil {

    public static void runStringCommand(final String command) throws IOException, InterruptedException {
        final int returncode = Runtime.getRuntime().exec(new String[] { "bash", "-c", command }).waitFor();
        if (returncode != 0) {
            System.err.println("The script returned an Error with exit code: " + returncode);
            //throw new IOException();
        }
    }

    /**
     * Run script.
     *
     * @param scriptFile the script file
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws InterruptedException the interrupted exception
     */
    public static void runScript(final String scriptFile) throws IOException, InterruptedException {
        final String command = scriptFile;
        if (!new File(command).exists() || !new File(command).canRead() || !new File(command).canExecute()) {
            System.err.println("Cannot find or read " + command);
            System.err.println("Make sure the file is executable and you have permissions to execute it. Hint: use \"chmod +x filename\" to make it executable");
            throw new IOException("Cannot find or read " + command);
        }
        final int returncode = Runtime.getRuntime().exec(new String[] { "bash", "-c", command }).waitFor();
        if (returncode != 0) {
            System.err.println("The script returned an Error with exit code: " + returncode);
            throw new IOException();
        }
    }

    public List<String> getGroupHashtagList(int groupNum, boolean localRun) {
        List<String> hashtagList = new ArrayList<>();
        if(localRun){
            hashtagList.add("h1");
            hashtagList.add("h5");
            hashtagList.add("h9");
        }else {
            String hashtagStrList = "";
            if (groupNum == 1)      //NATURAL DISASTER
                hashtagStrList = "earthquake,haiyan,storm,tornado,prayforthephilippines,ukstorm,sandy,flood,drought,hurricane,arthur,tsunami,kashmirfloods,phailin,hurricanes,quake,typhoon,eqnz,prayforchile,katrina,bertha,typhoonhaiyan,serbiafloods,julio,hurricanearthur,manuel,cholera,napaquake,hurricanesandy,odile,earthquakeph,ukfloods,typhoonaid,abfloods,hurricaneseason,hurricaneseason,chileearthquake,hurricaneprep,laquake,hurricanegonzalo,hurricanekatrina,typhoonhagupit,corkfloods,hurricaneodile,laearthquake,floodwarning,napaearthquake,tsunami2004,tsunamimarch";
            else if (groupNum == 2) // EPIDEMICS
                hashtagStrList = "health,uniteblue,ebola,healthcare,depression,hiv,cdc,crisis,obesity,aids,nurse,flu,alert,publichealth,bandaid30,malaria,disease,fever,antivirus,virus,lagos,unsg,sierraleone,ebolaresponse,ebolaoutbreak,chanyeolvirusday,aids2014,vaccine,mer,homeopathy,msf,allergy,nih,humanitarianheroes,stopthespread,dengue,flushot,epidemic,ebolainatlanta,tuberculosis,westafrica,quarantine,ebolavirus,viruses,kacihickox,emory,meningitis,ebolaczar,enterovirus,pandemic,stopebola,chikungunya,eplague,childhoodobesity,plague,allergyseason,coronavirus,healthworkers,endebola,ebolaqanda,obola,h1n1,aidsfree,factsnotfear,ebolafacts,chickenpox,birdflu,ebolainnyc,dallasebola,ebolachat,eboladallas,childobesity,healthsystems,aidsday,truedepressioniswhen,askebola,depressionawareness,ambervinson,depressionhurts,ninapham,nursesfightebola,mickeyvirus,rotavirus,blackdeath,theplague";
            else if (groupNum == 3) //IRAN TALK
                hashtagStrList = "irantalks,rouhani,iranian,irantalksvienna,nonucleariran,irannews,irandeal,irantalksnyc,iranfreedom,irani,nuclearweapons,irantalksoman,irantalk,nuclearenergy,iranhrviolations,iranianssupport,nuclearpower";
            else if (groupNum == 4) // SOCIAL ISSUES
                hashtagStrList = "racism,mikebrown,shutitdown,icantbreathe,ferguson,nojusticenopeace,moa,policebrutality,antoniomartin,thesystemisbroken,justice4all,michaelbrown,blacklivesmatter,blackxmas,ericgarner,justiceformikebrown,handsupdontshoot,alllivesmatter,thisstopstoday,fergusondecision,tamirrice,policelivesmatter,berkeleyprotests,millionsmarchnyc,aurarosser,nypdlivesmatter,abortion,debt,gunlaws,legalize,legalizemarijuana,nationaldebt,abortions,debts,endabortion,debtceiling,legalizecannabis,legalweed,stopabortion,legalized,freetheweed,abortionaccess,abortionismurder,newnjgunlaws,newnjgunlaw,abortionvote,44millionabortions,safeabortion,legalize420,nonewnjgunlaws";
            else if (groupNum == 5) // LBGT
                hashtagStrList = "tcot,p2,pjnet,uniteblue,teaparty,2a,ccot,equality,marriageequality,tgdn,pride,stoprush,loveislove,popefrancis,vatican,legalizeit,gaymarriage,legalize,wapol,homo,equality4all,ssm,ibdeditorials,gaypride,equalityforall,wakeupamerica,samesexmarriage,lovewins,homosexuality,ally,homosexual,alliances,equalitymatters,marylandpride,legalizegayma,homos,acceptancematters,gaylove,sacksheila,gaymoment,equalityformen,unitebluemt,gaymen,sacks,equalitynow,legalizegay";
            else if(groupNum == 6){ // HUMAN CAUSED DISASTER
                hashtagStrList ="syria,gaza,isis,israel,mh370,gazaunderattack,mh17,palestine,freepalestine,is,bringbackourgirls,prayforgaza,iss,hamas,prayformh370,isil,taliban,syrian,southsudan,bds,icc4israel,younusalgohar,israeli,palestinian,idf,malala,malaysiaairlines,sudan,bokoharam,palestinians,jamesfoley,jamesfoley,chibokgirls,daesh,alqaeda,childrenofsyria,ajagaza,rafah,notinmyname,gazaunderfire,freesyria,withsyria,abuja,nowarwithsyria,farc,ripmh370,drugwar,syriawarcrimes,stopwar,bombsquad,handsoffsyria,malnutrition,chibok,juba,bringourgirlsback,southsudannow,whereisthefuckingplane,cholera,antiwar,realsyria,savesyria,isismediablackout,alshabab,iraqwar,nigerianschoolgirls,ripjamesfoley,famine,bronxbombers,bringbackourdaughters,igad,bringbackourgirl,helpsyriasrefugees,bostonmarathonbombing,redefinenigeria,234whitegirls,bombthreat,stayoutofsyria,bentiu";
            }else if(groupNum == 7) // CELEBRITY DEATH
                hashtagStrList = "jamesavery,freshprince,unclephil,freshprinceofbelair,rip,ripjamesavery,thefreshprinceofbelair,robinwilliams,nelsonmandela,philipseymourhoffman,paulwalker,mandela,prayforap,madiba,mayaangelou,rippaulwalker,riprobinwilliams,ripnelsonmandela,ripcorymonteith,ripmandela,ripjoanrivers,riptalia,riplilsnupe,ripleerigby,riprise,ripmaeyoung,ripshain,ripeunb,riposcardelarenta,riplarryshippers,ripkelcey,riptitovilanova,ripsimone,riptrayvonmartin,ripmayaangelou,ripmadiba,ripallisonargent,ripunclephil,ripmitchlucker,riprogerebert,ripjamesfoley,ripshaingandee,ripphilipseymourhoffman,riplaurenbacall";
            else if(groupNum == 8) // SPACE
                hashtagStrList = "1yearonmars,aerospace,aliens,antares,apollo,apollo11,apollo13,apollo45,armstrong,asknasa,asteroid,asteroids,astr,astro,astrobiology,astrology,astronaut,astronauts,astronomy,atlantis,auroras,blackhole,blackholefriday,blackholes,bloodmoon,bloodmooneclipse,bluemoon,bluemoontourenchile,cassini,clubpluto,comet,cometlanding,comets,cosmos,curiosity,cygnus,darksideofthemoon,discovery,earth,earthday,earthrightnow,eft1,exoplanets,exp40,exp41,extraterrestrial,flight,fullmoon,fullmoonparty,gagainspace2015,get2space,gocomets,gravity,harvestmoon,houston,houstonwehaveaproblem,hubble,inspace,internationalspacestation,interstellar,iris,isee3,iss,isscrew,journeytomars,jupiter,kepler,killthemoon,ladee,landsat,livefromspace,lunar,lunareclipse,mars,marsiscoming,marsmission,marte,maven,meteor,meteorgarden,meteorite,meteorites,meteorito,meteorjs,meteorology,meteors,meteorshower,meteorwatch,missiontomars,moon,moonday,moonlanding,moonlight,moons,nasa,nasasocial,nasatv,nasatweetup,newmoon,nextgiantleap,orb3,orion,orionlaunch,outerspace,perseidmeteorshower,planet,planetearth,planets,planetsunburn,pluto,projectloon,redmoon,rocket,rockets,russianmeteor,satellite,satellites,saturn,science,scientist,scientists,scifi,scifinow,solar,solarsystem,space,spacebound,spacecraft,spaceinvaders,spacelive,spaceman,spacemigrationtour,spaces,spaceship,spaceshiptwo,spacestation,spacetoground,spacetravel,spacewalk,spaceweather,spacex,spacex3,stars,starship,startrek,starwars,stem,sun,supermoon,supermoon2014,supernova,sxsw,telescope,themoon,thirtysecondstomars,universe,upintheair,venus,visitjsc,votemars,voyager1";
            else if(groupNum == 9) // TENNIS
                hashtagStrList = "usopenxespn,vansusopen,womensausopen,usopen,usopen13,usopen14,usopen201,usopen2013,vansusopen,usopen2014,usopenchampion,usopencup,usopenfinal,djokovic,djokovicvsfederer,djokovicvsmurray,federervsdjokovic,nadaldjokovic,novakdjokovic,teamdjokovic,novak,teamnovak,frenchopen,frenchopen2013,frenchopen2014,frenchopenfinal,frenchopentennis,australianopen,australianopen2014,atptennis,espntennis,lovetennis,niketennis,tennis,afcwimbledon,bbcwimbledon,espnwimbledon,lovewimbledon,sendmetowimbledon,wearewimbledon,wimbledonfinal,wimbledon,wimbledontennis,wimbledonxespn,wimbledon13,wimbledon2013,wimbledon2014,wimbledon2o13,wimbledonchamp,wimbledonchampion,wimbledone,wimbledonfinal2013,wimbledonfinals,whenandywonwimbledon,atpmadrid,atpmasters,atpmontecarlo,atptennis,atpsunday,atptour,atptourfinals,atpworldtour,atpworldtourfinal,atpworldtourfinals,usopenseries,usopentennis,federervsnadal,murraynadal,nadal,nadaldjokovic,nadalfederer,nadalferrer,rafaelnadal,rafanadal,rafanadaltour,teamnadal,vamosnadal,womenstennis,canadiantennis,chutennis,tenniscanada,cincytennis,tennischannel,collegetennis,tenniscourt,dubaitennis,tenniscourts,eurosporttennis,tenniselbow,tennisiscanada,tennisnews,sydneytennis,teamfrancetennis,tennisball";
            else if(groupNum == 10) // SOCCOR
                hashtagStrList = "soccer,football,worldcup,sports,futbol,fifa,mls,worldcup2014,epl,sportsroadhouse,sport,adidas,messi,usmnt,arsenal,manchesterunited,nike,ronaldo,manutd,fifaworldcup,foot,ussoccer,sportsbetting,realmadrid,aleague,chelsea,manchester,cr7,footballnews,championsleague,youthsoccer,eplleague,barcelona,brazil2014,soccerproblems,premierleague,brasil2014,soccerlife,cristianoronaldo,uefa,fifa2014,beckham,fifa14,neymar,fussball,soccergirls,barca,manchestercity,league,fútbol,halamadrid,bayern,women,lfc,goalkeeper,everton,bayernmunich,soccerprobs,league1,juventus,nufc,mcfc,cristiano,eurosoccercup,platini,socce,mancity,torontofc,dortmund,derbyday,fifa15,liverpool,league2,ilovesoccer,fcbarcelona,maradona,intermilan,futebol,soccergirlprobs,soccersixfanplayer,realfootball,gunners,confederationscup,worldcupproblems,ballondor,collegesoccer,rooney,flagfootball,realsaltlake,lionelmessi,usavsportugal,europaleague,soccernews,uefachampionsleague,psg,gobrazil,uslpro,wc2014,suarez,bvb,soccerprobz,worldcupqualifiers,torres,footbal,balotelli,nashville,inter,milano,cardiff,jleague,nwsl,ozil,worldcup2014brazil,nycfc,mess,soccernation,pelé,tottenham,ligue1,landondonovan,atletico,worldcup14,torino,soccerislife,fernandotorres,ronaldinho,goldenball,wembley,brazilvscroatia,collegefootball,elclassico,footba,fifa13,soccersunday,englandsoccercup,usasoccer,womensfootball,fcbayern,fifaworldcup2014,usavsgermany,neymarjr,soccersucks,arturovidal,zidane,ballislife,usavsger,mlscup,worldcupfinal,ajax,soccerball,lovesoccer,euro2013,soccergame,premiereleague,mu,lionel,soccermanager,mundial2014,portugalvsgermany,soccerseason,mondiali2014,davidbeckham,redbulls,argvsned,selecao,usavsmex,soccergirlproblems,soccerlove,2014worldcup,soccergrlprobs,germanyvsargentina,zlatan,napoli,muller,confederations_cup,championsleaguefinal,worldcuppredictions,clasico,liverpoolvsrealmadrid,mundialsub17,worldcupbrazil,leaguechamps,arsenalfans,germanyvsalgeria,netherlandsvsargentina,belvsusa,bravsned,mexicovsusa,englandvsuruguay,germanyvsbrazil,brazilvsnetherlands,gervsarg,engvsita,brazilvsgermany,englandvsitaly,espvsned,crcvsned,ghanavsusa,francevsswitzerland,argentinavsgermany,spainvsnetherlands,usavscan,worldcupbrazil2014,brazil2014worldcup,fifaworldcupbrazil,worldcup2018,championleague";

            Collections.addAll(hashtagList, hashtagStrList.split(","));
        }
        return hashtagList ;
    }
}
