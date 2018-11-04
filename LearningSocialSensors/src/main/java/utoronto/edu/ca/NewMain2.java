/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utoronto.edu.ca;

import java.io.IOException;
import me.tongfei.progressbar.ProgressBar;

/**
 *
 * @author rbouadjenek
 */
public class NewMain2 {

    public static void main(String[] args) throws IOException, InterruptedException {
        // try-with-resource block
        try (ProgressBar pb = new ProgressBar("Test", 1000)) { // name, initial max
            // Use ProgressBar("Test", 100, ProgressBarStyle.ASCII) if you want ASCII output style
            for (int x = 0; x < 1000; x++) {
                    pb.step(); // step by 1
                    pb.step(); // step by 1

                    pb.setExtraMessage("Reading..."); // Set extra message to display at the end of the bar
                
                Thread.sleep(100);
//                pb.maxHint(n);
                // reset the max of this progress bar as n. This may be useful when the program
                // gets new information about the current progress.
                // Can set n to be less than zero: this means that this progress bar would become
                // indefinite: the max would be unknown.
            }
        } // progress bar stops automatically after completion of try-with-resource block
    }

}
