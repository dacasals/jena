package cl.uc.ing.benchmark;

import java.io.*;

public class Endpoint {
    public static void main(String[] args) {
        String[] params = new String[args.length-2];
        System.arraycopy(args, 2, params, 0, args.length - 2);
        //Read File
        try {
            File file = new File(args[1]);

            BufferedReader br = new BufferedReader(new FileReader(file));

            String st;
            while ((st = br.readLine()) != null)
                System.out.println(st);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if ("Find_J3".equals(args[0])) {

            if(true)
                cl.uc.ing.benchmark.tdb2.Find_J3.main(params);
            else Find_J3.main(params);
        }
        else if ("Find_P2".equals(args[0])) {
            if(true)
                cl.uc.ing.benchmark.tdb2.Find_P2.main(params);
            else Find_P2.main(params);
        }
        else if ("Find_S1".equals(args[0])) {
            if(true)
                cl.uc.ing.benchmark.tdb2.Find_S1.main(params);
            else Find_S1.main(params);
        }
        else if ("Find_S2".equals(args[0])) {
            if(true)
                cl.uc.ing.benchmark.tdb2.Find_S2.main(params);
            else Find_S2.main(params);
        }
        else if ("Find_S3".equals(args[0])) {
            if(true)
                cl.uc.ing.benchmark.tdb2.Find_S3.main(params);
            else Find_S3.main(params);
        }
        else if ("Find_S4".equals(args[0])) {
            if(true)
                cl.uc.ing.benchmark.tdb2.Find_S4.main(params);
            else Find_S4.main(params);
        }
        else if ("Find_T2".equals(args[0])) {
            if(true)
                cl.uc.ing.benchmark.tdb2.Find_T2.main(params);
            else Find_T2.main(params);
        }
        else if ("Find_T3".equals(args[0])) {
            if(true)
                cl.uc.ing.benchmark.tdb2.Find_T3.main(params);
            else Find_T3.main(params);
        }
        else if ("Find_TI2".equals(args[0])) {
            if(true)
                cl.uc.ing.benchmark.tdb2.Find_TI2.main(params);
            else Find_TI2.main(params);
        }
        else if ("Find_TI3".equals(args[0])) {
            if(true)
                cl.uc.ing.benchmark.tdb2.Find_TI3.main(params);
            else Find_TI3.main(params);
        }
        else if ("Find_Tr1".equals(args[0])) {
            if(true)
                cl.uc.ing.benchmark.tdb2.Find_Tr1.main(params);
            else Find_Tr1.main(params);
        }
        else if ("Find_Tr2".equals(args[0])) {
            if(true)
                cl.uc.ing.benchmark.tdb2.Find_Tr2.main(params);
            else Find_Tr2.main(params);
        }
        else {
            System.out.print("Tarea no encontrada");
        }
    }
}
