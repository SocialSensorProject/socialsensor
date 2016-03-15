package machinelearning;

/**
 * Created by zahraiman on 3/14/16.
 */
public class Feature implements Comparable<Feature>{
    private String featureName;
    private double featureWeight;

    public Feature(String featureName, double featureWeight) {
        this.featureName = featureName;
        this.featureWeight = featureWeight;
    }

    public String getFeatureName() {
        return featureName;
    }

    public void setFeatureName(String featureName) {
        this.featureName = featureName;
    }

    public double getFeatureWeight() {
        return featureWeight;
    }

    public void setFeatureWeight(double featureWeight) {
        this.featureWeight = featureWeight;
    }

    @Override
    public int compareTo(Feature o) {
        if(o.getFeatureWeight() < this.getFeatureWeight())
            return -1;
        else if(o.getFeatureWeight() > this.getFeatureWeight())
            return 1;
        else
            return 0;
    }
}
