package recommend;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException {
        FileInputStream i = new FileInputStream("src/main/resources/conf.properties");
        int read = 0;

        while ((read = i.read())!=-1){
            System.out.print(read);
        }
    }
}
