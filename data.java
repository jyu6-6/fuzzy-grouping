import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Random;
import com.github.javafaker.Faker;

public class data {
    public static void main(String[] args) {
        try {
            File file = new File("/Users/maggiemin/Documents/Big_data_management/final_project/customer100.txt");
            FileWriter fw = new FileWriter(file);
            BufferedWriter bw = new BufferedWriter(fw);
            Faker faker = new Faker();
            for (int ID = 1; ID <= 4000000; ID = ID+4) {
                String firstName = faker.name().firstName();
                String lastName = faker.name().lastName();
                String[] mid = {faker.name().firstName(),faker.name().lastName()};
                Random rand = new Random();
                String middleName = mid[rand.nextInt(mid.length)];
                String abbr_middle = String.valueOf(Character.toUpperCase(middleName.charAt(0)))+'.';
                bw.write(String.valueOf(ID) + ',' + firstName + ' ' + lastName);
                bw.newLine();
                bw.write(String.valueOf(ID+1) + ',' + lastName + ' ' + firstName);
                bw.newLine();
                bw.write(String.valueOf(ID+2) + ',' + firstName + ' ' + middleName+' '+ lastName);
                bw.newLine();
                bw.write(String.valueOf(ID+3) + ',' + firstName + ' ' + abbr_middle+' '+ lastName);
                bw.newLine();
            }
            bw.close();
            fw.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

