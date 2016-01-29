package visualization1;

/**
 * Created by zahraiman on 9/9/15.
 */
public class ScatterPlot implements Comparable<ScatterPlot> {

    public double featureValue;
    public double secondDimCount;
    public String featureKey;
    public boolean flagCE;



    public double getFeatureValue() {
        return featureValue;
    }

    public void setFeatureValue(double featureValue) {
        this.featureValue = featureValue;
    }

    public double getSecondDimCount() {
        return secondDimCount;
    }

    public void setSecondDimCount(double secondDimCount) {
        this.secondDimCount = secondDimCount;
    }

    public String getFeatureKey() {
        return featureKey;
    }

    public void setFeatureKey(String featureKey) {
        this.featureKey = featureKey;
    }

    @Override
    public int compareTo(ScatterPlot o) {
        if(!flagCE) {
            if (this.featureValue > o.featureValue)
                return -1;
            else if (this.featureValue < o.featureValue)
                return 1;
            else
                return 0;
        }else{
            if (this.featureValue > o.featureValue)
                return 1;
            else if (this.featureValue < o.featureValue)
                return -1;
            else
                return 0;
        }
    }

    /*@Override
    public int compareTo(ScatterPlot o) {
        if(!flagCE) {
            if (this.secondDimCount > o.secondDimCount)
                return -1;
            else if (this.secondDimCount < o.secondDimCount)
                return 1;
            else
                return 0;
        }else{
            if (this.secondDimCount > o.secondDimCount)
                return 1;
            else if (this.secondDimCount < o.secondDimCount)
                return -1;
            else
                return 0;
        }
    }*/

    public ScatterPlot(double featureValue, double secondDimCount, String featureKey, boolean flagCE) {
        this.featureValue = featureValue;
        this.secondDimCount = secondDimCount;
        this.featureKey = featureKey;
        this.flagCE = flagCE;
    }
}
