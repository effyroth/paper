/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author hadoop
 */
public class testqrt {
    public static void main(String Args[]){
        Double kr = 16.0;
        Long F1 = 120001l;
        Long D1 = 30000l;
        Long sum = F1 * D1;
        Double bucketside = Math.sqrt( sum / kr);
        Double F1segments = (F1 - 1)  / bucketside + 1;
        Double D1segments = (D1 - 1)  / bucketside + 1;
        kr = F1segments * D1segments;
        int i = 0;
    }
}
