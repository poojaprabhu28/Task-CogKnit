package com.github.poojaprabhu28.cogknit;
//reads data from input file

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class FileReadData {
    String lines = "";

    //Open audio_data.txt and read data
    public String readData(String filepath)
    {
        try{
            FileReader fr = new FileReader(filepath);
            BufferedReader br = new BufferedReader(fr);
            String currentline = br.readLine();
            currentline = br.readLine();

            while(currentline != null) {
                lines += currentline + "\n";
                currentline = br.readLine();
            }
            br.close();
            fr.close();
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        //file data stored in lines
        return(lines);
    }
}
