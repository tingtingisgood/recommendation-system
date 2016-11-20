package mylab1;

import java.io.*;
import java.lang.ArrayIndexOutOfBoundsException;
import java.util.HashMap;
import java.util.Scanner;

public class Preprocess {
	
	public static void main(String[] args) throws IOException{
		preprocessMovieData(2000);
		preprocessRatingData();		 
	}
	
	//change index to be continuous and intercept the top K movies'records.
	public static void preprocessMovieData(int k) throws IOException{
		String pathIn="/home/yangtingting/recommendationsystem/datafiles/RawMovies.txt";
		String pathOut="/home/yangtingting/recommendationsystem/datafiles/Movies.txt";
		
		File file=new File(pathOut);
		if(!file.exists()){
			file.createNewFile();
		}
		else System.err.println("Movies.txt already existed!");	
		try{
			FileReader fr=new FileReader(pathIn);
			BufferedReader br=new BufferedReader(fr);
			FileWriter fw=new FileWriter(pathOut);
			PrintWriter pw=new PrintWriter(fw);
			String temp="";
			int count=1;
			while((temp=br.readLine())!=null){
				if(count==(k+1)) break;	
				String[] str=temp.split("\t",2);	
				pw.write((""+count+"\t"+str[1]+"\n").toCharArray());
				count++;
			}
			System.out.println("the total number of movie is: "+(count-1));
			br.close();
			pw.close();
		}catch(FileNotFoundException ee){
			ee.printStackTrace();
		}catch(ArrayIndexOutOfBoundsException ee){
			ee.printStackTrace();
		}
	}

	//change index to be continuous 
	public static void preprocessRatingData() throws IOException{
		String pathIn1="/home/yangtingting/recommendationsystem/datafiles/MovieId.txt";
		String pathIn2="/home/yangtingting/recommendationsystem/datafiles/RawRatings.txt";
		String pathOut1="/home/yangtingting/recommendationsystem/datafiles/MapTable.txt";
		String pathOut2="/home/yangtingting/recommendationsystem/datafiles/Ratings.txt";
		
		File file1=new File(pathOut1);
		if(!file1.exists()){
			file1.createNewFile();
		}
		else System.err.println("MapTable.txt already existed!");	
		
		File file2=new File(pathOut2);
		if(!file2.exists()){
			file2.createNewFile();
		}
		else System.err.println("Ratings.txt already existed!");	
		try{
			HashMap<String,String> map=new HashMap<String,String>();
			Scanner sc=new Scanner(new File(pathIn1)).useDelimiter(",");
			FileWriter fw1=new FileWriter(pathOut1);
			PrintWriter pw1=new PrintWriter(fw1);
			int count=1;
			String temp1="";
			while(sc.hasNext()){
				temp1=sc.next();
				if(count==3678) break;
				map.put(temp1,""+count);
				pw1.write((temp1+"\t"+count+"\n").toCharArray());
				count++;
			}
			FileReader fr=new FileReader(pathIn2);
			BufferedReader br=new BufferedReader(fr);
			FileWriter fw2=new FileWriter(pathOut2);
			PrintWriter pw2=new PrintWriter(fw2);
			String temp2="";
			while((temp2=br.readLine())!=null){
				String[] str=temp2.split("\t");
				//if(Integer.parseInt(str[0])>k) break;  //intercept the top 500 users' records
				if(Integer.parseInt(map.get(str[1]))>2000) continue;     
				pw2.write((""+str[0]+"\t"+map.get(str[1])+"\t"+str[2]+"\n").toCharArray());
			}
			sc.close();
			br.close();
			pw1.close();
			pw2.close();
		}catch(FileNotFoundException ee){
			ee.printStackTrace();
		}catch(ArrayIndexOutOfBoundsException ee){
			ee.printStackTrace();
		}
	}		
}


