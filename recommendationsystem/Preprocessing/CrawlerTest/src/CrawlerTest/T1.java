package CrawlerTest;

import java.io.IOException;

public class T1 {

	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	
		//String url = "http://www.imdb.com/title/tt0114709/";
		crawler cr = new crawler();

		cr.spider();
		//cr.overprocess();
	}

}
