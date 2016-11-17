package CrawlerTest;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;


public class crawler {

	//String url = null;
	protected HashSet<String> actorlist;		// store actors appeared
	protected HashSet<String> directorlist;		// store directors appeared
	protected HashSet<String> langlist;			// store languages appeared
	protected Queue<String> queue;		// store url prepared to be crawlered
	//protected Map<String, Integer> tUrlToId = new HashMap<String,Integer>();
	protected Map<String, String> UrlToId = new HashMap<String,String>();
	protected List<Map.Entry<String,String>> UrlToIdlist;
	protected File mainfile;
	protected File actorfile;
	protected File directorfile;
	protected File langfile;
	protected String filepath;
	protected String excelname;
	protected HSSFWorkbook wb;
    protected HSSFSheet sheet;
    protected File fileexcel;
    protected int index=0;
    protected int direIndex = 1, actorIndex = 1, langIndex = 1;
    protected String movieID;
	
	public crawler() throws IOException{
		//this.url = url;
		directorlist = new HashSet<String>();
		actorlist = new HashSet<String>();	
		langlist = new HashSet<String>();
		
		this.ReadUrlFile();
			
		/*
		this.InitialFile();
		this.WriteIntoFile(mainfile,"movieID", "Year", "Director", "Actors", "Languages");
		this.WriteIntoFile(actorfile, "actorID", "Actor");
		this.WriteIntoFile(directorfile, "directorID", "Director");
		this.WriteIntoFile(langfile, "languageID", "Language");
		*/
		this.InitialFile2();
	}
	
	
	public void spider() throws IOException, InterruptedException{
		String newAddress;
		for (Map.Entry<String, String> k : UrlToIdlist){
			newAddress = k.getKey();
			if (isValid(newAddress)){
				movieID = k.getValue();
				System.out.println("Enter a new page: "+newAddress);
				processPage(newAddress);
			}
		}
	}
	
	public void processPage(String url) throws IOException, InterruptedException{
		Document doc;
		while (true){
			try{
				doc = Jsoup.connect(url).timeout(0).userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36")
					     .get();
				break;
			}
			catch(HttpStatusException e){
				e.printStackTrace();
				return;
			}
		}	
		System.out.println("-------Start------");
		
		Elements year = doc.select("#titleYear").select("a");
		Elements directors = doc.select("span[itemprop=director]").select("span[class=itemprop]");
		Elements actors = doc.select("span[itemprop=actors]").select("span[class=itemprop]");
		Elements languages = doc.select("a[href~=languages]");
		
		int yearCount = 0, direCount = 0, actorCount = 0, langCount = 0;
		String yearString = "", direString = "", actorString = "", langString = "";
		
		if((yearCount=year.size())!=0){
			yearString = year.get(0).text();
		}
		if((direCount=directors.size())!=0){
			direCount = (direCount > 1) ? 1 : direCount;
			for (int i = 0; i < direCount; i++){
				String curDirector = directors.get(i).text();
				if (i == 0){
					direString = curDirector;
				}
				else{
					direString = direString + '|' + curDirector;
					System.out.println(directors.get(i).text());
				}
				if (!directorlist.contains(curDirector)){
					directorlist.add(curDirector);
					this.WriteIntoFile(directorfile, ""+direIndex, curDirector);
					direIndex++;
				}
			}
			//this.WriteIntoExcel(index++, content.text());
			//this.WriteIntoFile(title.text() + "\t" + content.text());
		}
		if((actorCount=actors.size())!=0){
			actorCount = (actorCount > 3) ? 3 : actorCount;
			for (int i = 0; i < actorCount; i++){
				String curActor = actors.get(i).text();
				if (i == 0){
					actorString = curActor;
				}
				else{
					actorString = actorString + '|' + curActor;
					System.out.println(actors.get(i).text());
				}
				if (!actorlist.contains(curActor)){
					actorlist.add(curActor);
					this.WriteIntoFile(actorfile, ""+actorIndex, curActor);
					actorIndex++;
				}
			}
			
			//this.WriteIntoExcel(index++, content.text());
			//this.WriteIntoFile(title.text() + "\t" + content.text());
		}
		if((langCount=languages.size())!=0){
			for (int i = 0; i < langCount; i++){
				String curLang = languages.get(i).text();
				if (i == 0){
					langString = curLang;
				}
				else{
					langString = langString + '|' + curLang;
					System.out.println(curLang);
				}
				if (!langlist.contains(curLang)){
					langlist.add(curLang);
					this.WriteIntoFile(langfile, ""+langIndex, curLang);
					langIndex++;
				}
			}
			//this.WriteIntoExcel(index++, content.text());
			//this.WriteIntoFile(title.text() + "\t" + content.text());
		}
		System.out.println("movieID:" + movieID);
		System.out.println("year: " + yearString);
		System.out.println("directors: " + direString);
		System.out.println("actors: " + actorString);
		System.out.println("languages: " + langString);
		System.out.println("-------Over-----");
		//this.WriteIntoExcel(index++, movieID, yearString, direString, actorString, langString);		
		this.WriteIntoFile(mainfile, movieID, yearString, direString, actorString, langString);
	}
	
	public void InitialFile() throws IOException{
		filepath = System.getProperty("user.dir") + "/mainfile.csv" ;
		mainfile = new File(filepath);
		if (!mainfile.exists()){
			mainfile.createNewFile();
		}
		else{
			mainfile.delete();
			mainfile.createNewFile();
		}
		
		filepath = System.getProperty("user.dir") + "/actorfile.csv" ;
		actorfile = new File(filepath);
		if (!actorfile.exists()){
			actorfile.createNewFile();
		}
		else{
			actorfile.delete();
			actorfile.createNewFile();
		}
		
		filepath = System.getProperty("user.dir") + "/directorfile.csv" ;
		directorfile = new File(filepath);
		if (!directorfile.exists()){
			directorfile.createNewFile();
		}
		else{
			directorfile.delete();
			directorfile.createNewFile();
		}
		
		filepath = System.getProperty("user.dir") + "/languagefile.csv" ;
		langfile = new File(filepath);
		if (!langfile.exists()){
			langfile.createNewFile();
		}
		else{
			langfile.delete();
			langfile.createNewFile();
		}
	}
	

	public void InitialFile2() throws IOException{
		filepath = System.getProperty("user.dir") + "/mainfile.csv" ;
		mainfile = new File(filepath);
		
		filepath = System.getProperty("user.dir") + "/actorfile.csv" ;
		actorfile = new File(filepath);
		actorIndex = this.ReadFile(filepath, actorlist);
		
		filepath = System.getProperty("user.dir") + "/directorfile.csv" ;
		directorfile = new File(filepath);
		direIndex = this.ReadFile(filepath, directorlist);
		
		filepath = System.getProperty("user.dir") + "/languagefile.csv" ;
		langfile = new File(filepath);
		langIndex = this.ReadFile(filepath, langlist);
	}
	
	public boolean isValid(String url){
		return (url.startsWith("http://www.imdb.com/title/tt"));
	}
	
	public int ReadFile(String filepath, HashSet<String> list){
		BufferedReader br = null;
		String curLine;
		int index = 0;
		try {
			br = new BufferedReader(new FileReader(filepath));
			try {
				while ((curLine = br.readLine())!=null){
					index++;
					if (index > 1){
						System.out.println(curLine);
						String[] seg;
						seg = curLine.split(",");
						if (seg.length == 2){
							list.add(seg[1]);
						}
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return index;
	}
	
	public void ReadUrlFile(){
		BufferedReader br = null;
		String orifilepath = "/Users/fengpiaopiao/Documents/software/Eclipse/Projects/CrawlerTest/imdbLinks.txt";
		try{
			String curLine;
			br = new BufferedReader(new FileReader(orifilepath));
			while ((curLine = br.readLine()) != null){
				System.out.println(curLine);
				String[] seg;
				seg = curLine.split("\t");
				if (seg.length == 2){
					UrlToId.put(seg[1], seg[0]);
				}
			}
		}
		catch (IOException e){
			e.printStackTrace();
		}
		finally {
			try {
				if (br != null) br.close();
			}
			catch (IOException ex){
				ex.printStackTrace();
			}
		}
		/*
		System.out.println(UrlToId.size());
		for (String k : UrlToId.keySet()){
			System.out.println("key is:" + k);
			System.out.println("value is:" + UrlToId.get(k));
		}
		*/
		UrlToIdlist = new ArrayList<Map.Entry<String,String>>(UrlToId.entrySet());
		Collections.sort(UrlToIdlist, new Comparator<Map.Entry<String,String>>() {
			// sort ascent
			public int compare(Entry<String, String> o1, Entry<String,String> o2){
				int i1 = Integer.parseInt(o1.getValue()), i2 = Integer.parseInt(o2.getValue());
				if (i1 < i2)
					return -1;
				else if (i1 == i2)
					return 0;
				else return 1;
				//return o1.getValue().compareTo(o2.getValue());
			}
			
		});
		/*
		System.out.println("after sort");
		for (Map.Entry<String, String> k : UrlToIdlist){
			System.out.println("key is:" + k.getKey());
			System.out.println("value is:" + k.getValue());
		}
		*/
	}
	
	protected void WriteIntoFile(File file, String index, String content){
		FileWriter fw = null;
		try {
	
			fw = new FileWriter(file,true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		PrintWriter pw = new PrintWriter(fw);
		pw.println(index + "," + content);
		pw.flush();
		
		try {
			fw.flush();
			pw.close();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	protected void WriteIntoFile(File file, String movieID, String content1, String content2, String content3, String content4){
		FileWriter fw = null;
		try {
	
			fw = new FileWriter(file,true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		PrintWriter pw = new PrintWriter(fw);
		pw.println(movieID + "," + content1 + "," + content2 + "," + content3 + "," + content4);
		pw.flush();
		
		try {
			fw.flush();
			pw.close();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void WriteIntoExcel(int iRow, String content){
        
		HSSFRow row = sheet.createRow(iRow);  //

        HSSFCell cell = row.createCell(0);  
        
        cell.setCellValue(new HSSFRichTextString(content));  
   
        ByteArrayOutputStream os = new ByteArrayOutputStream();  
          
        try{  
            wb.write(os);  
        }catch(IOException e){  
            e.printStackTrace();  
            //return null;  
        }  
          
        byte[] xls = os.toByteArray();  

    //    File file = new File(excelname);  
        OutputStream out = null;  
        try {  
             out = new FileOutputStream(fileexcel);  
             try {  
                out.write(xls);  
            } catch (IOException e) {  
                // TODO Auto-generated catch block  
                e.printStackTrace();  
            }  
        } catch (FileNotFoundException e1) {  
            // TODO Auto-generated catch block  
            e1.printStackTrace();  
        }  
	}
	
	public void WriteIntoExcel(int iRow, String movieID, String content1, String content2, String content3, String content4){
        // create a new row
		HSSFRow row = sheet.createRow(iRow);  //
		
		// create a cell in new row
		HSSFCell cell0 = row.createCell(0);  
        cell0.setCellValue(new HSSFRichTextString(movieID)); 
		HSSFCell cell1 = row.createCell(1);  
        cell1.setCellValue(new HSSFRichTextString(content1)); 
        HSSFCell cell2 = row.createCell(2);  
        cell2.setCellValue(new HSSFRichTextString(content2)); 
        HSSFCell cell3 = row.createCell(3);  
        cell3.setCellValue(new HSSFRichTextString(content3)); 
        HSSFCell cell4 = row.createCell(4);  
        cell4.setCellValue(new HSSFRichTextString(content4)); 
		
        ByteArrayOutputStream os = new ByteArrayOutputStream();  
          
        try{  
            wb.write(os);  
        }catch(IOException e){  
            e.printStackTrace();  
            //return null;  
        }  
          
        byte[] xls = os.toByteArray();  

        // write the workbook in the file system
        OutputStream out = null;  
        try {  
             out = new FileOutputStream(fileexcel);  
             try {  
                out.write(xls);  
            } catch (IOException e) {  
                // TODO Auto-generated catch block  
                e.printStackTrace();  
            }  
        } catch (FileNotFoundException e1) {  
            // TODO Auto-generated catch block  
            e1.printStackTrace();  
        }  
	}
	
	public void overprocess(){
		int num_director = directorlist.size();
		int num_actor = actorlist.size();
		int num_lang = langlist.size();
		Iterator<String> Iter = directorlist.iterator();
		for (int i = 1; i <= num_director;i++){
			this.WriteIntoFile(directorfile, ""+i, Iter.next());
			//System.out.println(Iter.next());
		}
		
		Iter = actorlist.iterator();
		for (int i = 1; i <= num_actor;i++){
			this.WriteIntoFile(actorfile, ""+i, Iter.next());
			//System.out.println(Iter.next());
		}
		
		Iter = langlist.iterator();
		for (int i = 1; i <= num_lang;i++){
			this.WriteIntoFile(langfile, ""+i, Iter.next());
			//System.out.println(Iter.next());
		}
	}
	
}
