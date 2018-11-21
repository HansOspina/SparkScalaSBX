import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class Main {

    public static void main(String[] args) {

        int n = 5;

        int d = 3;


        int[] a = new int[]{1, 2, 3, 4, 5};

        int[] newArray = new int[a.length];


        for (int i = 0; i < a.length; i++) {
            newArray[(i + (a.length - d)) % a.length] = a[i];
        }


        for (int i = 0; i < newArray.length; i++) {
            System.out.print(newArray[i]);
            if (i + 1 < newArray.length) {
                System.out.print(" ");
            }
        }
    }

}
