/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eecs.oregonstate.edu.tweets;

/**
 *
 * @author rbouadjenek
 */
public class Main {

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        String prog = "unknown";
        if (args.length != 0) {
            prog = args[0].toLowerCase();
        }
        switch (prog) {
            case "count":
                Count.main(args);
                break;
            case "usercount":
                UserCount.main(args);
                break;
            case "hashtagbirthday":
                HashtagBirthday.main(args);
                break;
            case "hashtagbirthdayperuser":
                HashtagBirthdayPerUser.main(args);
                break;
            case "mentionnetwork":
                MentionNetwork.main(args);
                break;
            case "tweettokenizer":
                TweetTokenizer.main(args);
                break;
                case "tweets":
                Tweets.main(args);
                break;
            default:
                System.err.println("Bad usage. Use one of the following programs in the argument:");
                System.err.println("Count: Count the number of elements in a list.");
                System.err.println("UserCount: Extract unique users per number of tweets.");
                System.err.println("HashtagBirthday: Extract the birthday of each hashtag.");            
                System.err.println("HashtagBirthdayPerUser: Extract the hashtags birthday per users.");
                System.err.println("MentionNetwork: Extract the mention network.");
                System.err.println("TweetTokenizer: Tokenize all the tweets.");
                System.err.println("Tweets: Get a list of all the tweets.");

                break;
        }
    }

}
