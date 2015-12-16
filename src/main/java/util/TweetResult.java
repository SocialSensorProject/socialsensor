package util;

/**
 * Created by zahraiman on 12/9/15.
 */
public class TweetResult implements Comparable<TweetResult>{

    private long tid;
    private double weight;
    private String text;
    private int topical;

    public TweetResult(long tid, double weight, String text, int _topical) {
        this.tid = tid;
        this.weight = weight;
        this.text = text;
        this.topical = _topical;
    }

    public int getTopical() {
        return topical;
    }

    public void setTopical(int topical) {
        this.topical = topical;
    }

    public long getTid() {
        return tid;
    }

    public void setTid(long tid) {
        this.tid = tid;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public int compareTo(TweetResult o) {
        if(this.getWeight() < o.getWeight())
            return 1;
        else if(this.getWeight() > o.getWeight())
            return -1;
        else
            return 0;
    }
}
