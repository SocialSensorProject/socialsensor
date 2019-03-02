/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utoronto.edu.ca;

import java.text.ParseException;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

/**
 *
 * @author rbouadjenek
 */
public class NewMain2 { 

    public static void main(String[] args) throws ParseException {
        // try-with-resource block
        FastVector attributes = new FastVector();
        FastVector tempBooleanValues = new FastVector();
        tempBooleanValues.addElement(1);
        tempBooleanValues.addElement(2);
        attributes.addElement(new Attribute("attribute_1", tempBooleanValues));
        Instances dataRaw = new Instances("TestInstances", attributes,0);
        Instance instance = new Instance( 1);
        instance.setValue(0, 1);
        dataRaw.add(instance);
        System.out.println(dataRaw);

    }

}
