package mylab0;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;	//can used as identitymapper;
import org.apache.hadoop.mapreduce.Reducer; //can used as identityreducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class RecommendationSystem {
	
	private static String[] weight=new String[]{"0.20","0.20","0.20","0.20","0.20"};	//w1=0.25,w2=0.25,w3=0.25,w4=0.25,w5=0.20
	//private static double threshold=1.0;		//used to check if pass error evaluation
	private static int topK=3;	//intercept the topk movies as recommendation movies for each user.
	
	/*
     * function:compute the similarity between A and B
     * input data format:
     * output data format:
     */ 
	public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
		private String[] movieIdList;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			try{
				Path path= new Path("/user/hadoop/recommendationsystem/input/movieiddata/MovieId.txt");
				FileSystem fs=FileSystem.get(new Configuration());
				Scanner sc = new Scanner(fs.open(path));
				if(sc.hasNextLine()){
			        String line = sc.nextLine();
			        movieIdList = line.split(",");
		        }
		        sc.close();
		    }catch(Exception e){
		    	System.out.println(e);}
			for(String str:movieIdList){
				System.out.println(str);
			}
		}	
		
		@Override
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] tokens = values.toString().split("[\t]",2);
			//System.out.println("tokens[0] is:"+tokens[0]);
			//System.out.println("tokens[1] is:"+tokens[1]);
			//System.out.println("start write:");
			for(String movieId:movieIdList){
				//int a=Integer.parseInt(movieId);
				//System.out.println("tokens[0] is:"+tokens[0]);
				//System.out.println("type of tokens[0] is:"+(tokens[0] instanceof String));
				//int b=Integer.parseInt(tokens[0]);
				if(Integer.parseInt(movieId)<Integer.parseInt(tokens[0])) context.write(new Text(movieId+","+tokens[0]),new Text(tokens[1]));
				else if(Integer.parseInt(movieId)>Integer.parseInt(tokens[0])) context.write(new Text(tokens[0]+","+movieId),new Text(tokens[1]));
			}
			//System.out.println("end write:");
		}
   }
	
   
	/*
     * function:compute the similarity between any two movies. Do not include the similarity between itself.
     * input data format:<(movieid1,movieid2),"genre1 actor1 director1 language1 year1">
     *                   <(movieid1,movieid2),"genre2 actor2 director2 language2 year2">
     * output data format:<(movieid1,movieid2),"genreSimi,actorSimi,directorSimi,languageSimi,yearSimi">
     */ 
    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
	@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	    
		    String[] A=new String[5];		//store genre1 actor1 director1 language1 year1
	    	String[] B=new String[5];		//store genre2 actor2 director2 language2 year2
	    	//未考虑values.length不为2的异常
	    	Iterator<Text> iter=values.iterator();
	    	A=iter.next().toString().split("\t");
	    	B=iter.next().toString().split("\t");
	    	
	    	//genre1=A[0]
	    	//actor1=A[1]
	    	//director1=A[2]
	    	//language1=A[3]
			//year1=A[4];the same for B
	    	
	    	StringBuilder allSimilarity=new StringBuilder();	//made up of genre similarity,actor similarity,director similarity,language similarity,and year similarity
			
	    	//compute genre similarity
			int x=0,y=0,z=0;
			char[] A_genre=A[0].toCharArray();
			System.out.println("A[0]= "+A[0]);
			char[] B_genre=B[0].toCharArray();
			System.out.println("B[0]= "+B[0]);
			for(int i=0;i<18;i++){
				if(A_genre[i]=='1' && B_genre[i]=='1') z++;
				if(A_genre[i]=='1') x++;
				if(B_genre[i]=='1') y++;
			}
			double genreSimi=z/(Math.sqrt(x)*Math.sqrt(y));
			System.out.println("z= "+z);
			System.out.println("x= "+x);
			System.out.println("y= "+y);
			System.out.println("genreSimi= "+genreSimi);
			allSimilarity.append(""+genreSimi);
			//compute actor similarity
			double actorSimi=0;
			if(!A[1].equals("null") && !B[1].equals("null")){	
				String[] A_actor=A[1].split(",");		
				String[] B_actor=B[1].split(",");
				int p=0;
				for(int i=0;i<A_actor.length;i++){ 
					for(int j=0;j<B_actor.length;j++){
						if(A_actor[i].equals(B_actor[j])) p++;
					}
				}
		    actorSimi=p/(Math.sqrt(A_actor.length)*Math.sqrt(B_actor.length));
			}
			else actorSimi=0;	//when either A[1] or B[1] equals "null", actorSimi will set to 0;
			allSimilarity.append(","+actorSimi);
			//compute director similarity
			int directorSimi;
			if(A[2].equals(B[2])) directorSimi=1; 
			else directorSimi=0;
			allSimilarity.append(","+directorSimi);
			//compute language similarity
			int q=0;
			String[] A_language=A[3].split(",");
			String[] B_language=B[3].split(",");
			for(int i=0;i<A_language.length;i++){
				for(int j=0;j<B_language.length;j++){
					if(A_language[i].equals(B_language[j])) q++;
				}
			}
			double languageSimi=q/(Math.sqrt(A_language.length)*Math.sqrt(B_language.length));
			allSimilarity.append(","+languageSimi);
			//compute year similarity 
			int A4=Integer.parseInt(A[4]);
			int B4=Integer.parseInt(B[4]);
			double yearSimi;
			if(Math.abs(A4-B4)<=2) {
				yearSimi=1.00;}
			else if(Math.abs(A4-B4)<=4){
				yearSimi=0.75;}
			else if(Math.abs(A4-B4)<=6){
				yearSimi=0.50;}
			else if(Math.abs(A4-B4)<=8){
				yearSimi=0.25;}
			else yearSimi=0.00;
			allSimilarity.append(","+yearSimi);
			context.write(key,new Text(allSimilarity.toString()));
	    }
	}
    
   
    /*
    public static class Reducer2 extends Reducer<Text,Text,Text,DoubleWritable>{
    	private double[] w=new double[5];
    	@Override
    	protected void setup(Context context) throws IOException, InterruptedException {
		    Configuration conf=context.getConfiguration();
		    w[0]=Double.parseDouble(conf.get("w1"));
		    w[1]=Double.parseDouble(conf.get("w2"));
		    w[2]=Double.parseDouble(conf.get("w3"));
		    w[3]=Double.parseDouble(conf.get("w4"));
		    w[4]=Double.parseDouble(conf.get("w5"));
		}
    	
    	@Override
    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	    
		    Iterator<Text> iter=values.iterator();
		    String similarity="";
		    if(iter.hasNext()){
		    	similarity=iter.next().toString();
		    }
		    //genreSimi=tokens[0]
	    	//actorSimi=tokens[1]
	    	//directorSimi=tokens[2]
	    	//languageSimi=tokens[3]
		    //yearSImi=token[4];
		    //transform from the type String to  type double
		    String[] tokens=similarity.split(",");
		    double[] array=new double[5];
		    for(int i=0;i<tokens.length;i++){
		    	array[i]=Double.parseDouble(tokens[i]);
		    }
		    //compute the totalsimilarity
		    double totalSimilarity=0.0;
		    for(int i=0;i<5;i++){
		    	totalSimilarity+=w[i]*array[i];			
		    }
		    context.write(key,new DoubleWritable(totalSimilarity));
    	}
    }
    */
    
    
    /*
     * function:map every similarity value to every userId in the userIdList
     * input data format:<(movieid1,movieid2),"genreSimi,actorSimi,directorSimi,languageSimi,yearSimi">
     * output data format:<(userid,movieid1),"A:movieid2,genreSimi,acotrSimi,directorSimi,languageSimi,yearSimi"> 
     *                    <(userid,movieid2),"A:movieid1,genreSimi,acotrSimi,directorSimi,languageSimi,yearSimi">
     */ 
    public static class SimilarityMapper3 extends Mapper<Text, Text, Text, Text> {
		private List<String> userIdList=new ArrayList<String>(); //ArrayList supports the dynamically increase on length of array.
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			try{
				Path path= new Path("/user/hadoop/recommendationsystem/input/useriddata/UserId.txt");
				FileSystem fs=FileSystem.get(new Configuration());
				Scanner sc = new Scanner(fs.open(path));
				while(sc.hasNextLine()){
			        userIdList.add(sc.nextLine());
		        }
		        sc.close();
		    }catch(Exception e){
		    	System.out.println(e);}
	    }	
    
		@Override
		public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
			String[] tokens = key.toString().split(",");
			for(String userId : userIdList){
				context.write(new Text(userId+","+tokens[0]),new Text("A:"+tokens[1]+","+values.toString()));
			}
			for(String userId : userIdList){
				context.write(new Text(userId+","+tokens[1]),new Text("A:"+tokens[0]+","+values.toString()));
			}
		}
    }

    /*
     * function:map every rating value to every movieId in the movieIdList.
     * input data format:<key,"userid,movieid,rating")>
     * output data format:<(userid,movieid),"B:movieid,rating"> 
     */             
    public static class RatingsMapper3 extends Mapper<LongWritable, Text, Text, Text> {
		private  String[] movieIdList;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			try{
				Path path= new Path("/user/hadoop/recommendationsystem/input/movieiddata/MovieId.txt");
				FileSystem fs=FileSystem.get(new Configuration());
				Scanner sc = new Scanner(fs.open(path));
				if(sc.hasNextLine()){
			        String line = sc.nextLine();
			        movieIdList = line.split(",");
		        }
		        sc.close();
		    }catch(Exception e){
		    	System.out.println(e);}
		}
    
		@Override
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] tokens = values.toString().split("\t");
			for(String movieId:movieIdList){
				context.write(new Text(tokens[0]+","+movieId),new Text("B:"+tokens[1]+","+tokens[2]));
			}
		}
    }
    
    /*
     * function:compute the five subRatings respectively and write them to corresponding files respectively.
     * input data format:<(userid,movieid1),"A:movieid2,genreSimi,acotrSimi,directorSimi,languageSimi,yearSimi")
     *                   <(userid,movieid),"B:movieid,rating">
     * output data format:<(userid,movieid),"genreRating,actorRating,directorRating,languageRating,yearRating">
     */
    public static class Reducer3 extends Reducer<Text, Text, Text, Text> {
    	MultipleOutputs<Text,Text> mos;
    	
    	@Override
    	public void setup(Context context){
    		mos = new MultipleOutputs(context);
    	}
    	
	    @Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	    
		   
	    	Map<String, String[]> mapA = new HashMap<String, String[]>(); //store the <movieId, similarity between this movies and all the other movies>
			Map<String, String> mapB = new HashMap<String, String>(); //store the <movieId,ratings by the user>
			
			//organize the values into mapA and mapB respectively
			System.out.println("key= "+key.toString()+",values= "+values.toString());
			for (Text line : values) {
				String val = line.toString();	
				if (val.startsWith("A:")) {
					String[] kv = val.substring(2).split(",");	//substring(int beginIndex[,int endIndex])
					mapA.put(kv[0], new String[]{kv[1],kv[2],kv[3],kv[4],kv[5]});
					System.out.println("A rows: "+kv[0]+","+kv[1]+","+kv[2]+","+kv[3]+","+kv[4]+","+kv[5]);
				} else if (val.startsWith("B:")) {
					String[] kv = val.substring(2).split(",");
					mapB.put(kv[0], kv[1]);
					System.out.println("B rows: "+kv[0]+","+kv[1]);
				}
			}
			
			//compute the ratings
			Iterator<String> movieIdB = mapB.keySet().iterator();	//movieIdB stores all the movieId that current user has ever rated
			String[] k=key.toString().split(",");
			String movieId=k[1];	//movieId store the movieId that we are gonna predict rating for
			boolean exist=false;	//identify if to predict known rating or unknown rating	
			double[] numerator = new double[5];
			double[] denominator = new double[5];
			double[] ratings=new double[5];
			while (movieIdB.hasNext()) {
				String mapK = movieIdB.next();	//mapK store the movieId in the current iteration
				//check if ratings we are gonna compute is known rating or unknown rating,and if it is known rating,skip this iteration.
				if(mapK.equals(movieId)) {
					exist=true;
					continue;
				}
				for(int i=0;i<5;i++){
					numerator[i] += Double.parseDouble((mapA.get(mapK))[i]) * Double.parseDouble(mapB.get(mapK));
					denominator[i] +=Double.parseDouble((mapA.get(mapK))[i]);
				}		
			}
			
			//construct the output values
			StringBuilder allRating=new StringBuilder();	//store the string made up of genreRating,actorRating...yearRating
			if(denominator[0]==0) {
				allRating.append("null");}
			else{
				ratings[0]=((int)((numerator[0]/denominator[0])*100))/100.0;//conserve two decimal
				allRating.append(""+ratings[0]);
			}
			for(int i=1;i<5;i++){
				if(denominator[i]==0) {
					allRating.append(",null");}
				else{
					ratings[i]=((int)((numerator[i]/denominator[i])*100))/100.0;
					allRating.append(","+ratings[i]);
				}
			}
			
			//write the output key-value pair to the corresponding files
			if(exist){
				//compute rating and write it to the known file used for evaluation	
				mos.write("KnownRatings",key,new Text(allRating.toString()));}
			else{
				//compute rating and write it to the unknown file used for recommendation
				mos.write("UnknownRatings",key,new Text(allRating.toString()));
			}
	    }
	    
	    @Override
	    protected void cleanup(Context context)throws IOException, InterruptedException{
	    	mos.close();
	    }
	}  
    
    /*
     * function:calculate error.
     * input data fromat:<(userid,movieid),actual rating>  <(userid,movieid),predicted rating>
     * output data format:average error
     */ 
    /*
    public static class Mapper4 extends Mapper<LongWritable, Text, Text, Text> {
    
		@Override
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
			String[] tokens = values.toString().split("[\t|,]");
			context.write(new Text(tokens[0]+","+tokens[1]),new Text(tokens[2]));
		}
    }
    */
    
    /*
     * function:calculate the average error
     * input data fromat:<(userid,movieid),actual rating>  <(userid,movieid),predicted rating>
     * output data format:average error
     */
    /*
    public static class Reducer4 extends Reducer<Text,Text,NullWritable,DoubleWritable>{
    	
    	private double error=0.0;
    	private int count=0;
    	@Override
    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	    
		    //calculate the sum of error and count the number of records
    		double rating1=0.0;
    		double rating2=0.0;
    	    Iterator<Text> iter=values.iterator();
    	    //noted:lack exception process when length of values isnot 2,will cause error here.
    		rating1=Double.parseDouble(iter.next().toString());
    		rating2=Double.parseDouble(iter.next().toString());
    	    error+=Math.abs(rating1-rating2);
    	    count++;
    	}
    	
    	@Override
    	public void cleanup(Context context) throws IOException, InterruptedException{
    		context.write(NullWritable.get(),new DoubleWritable(error/count));
    	}
    }  
    */
    
    /*
     * function:compute the total rating with respect to weight to every subrating.
     */
    public static class Reducer4 extends Reducer<Text,Text,Text,DoubleWritable>{
    	private double[] w=new double[5];
    	@Override
    	protected void setup(Context context) throws IOException, InterruptedException {
		    Configuration conf=context.getConfiguration();
		    w[0]=Double.parseDouble(conf.get("w1"));
		    w[1]=Double.parseDouble(conf.get("w2"));
		    w[2]=Double.parseDouble(conf.get("w3"));
		    w[3]=Double.parseDouble(conf.get("w4"));
		    w[4]=Double.parseDouble(conf.get("w5"));
		}
    	
    	@Override
    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	    
		    Iterator<Text> iter=values.iterator();  
		    String[] tokens=iter.next().toString().split(",");
		    //genreRating=tokens[0]
	    	//actorRating=tokens[1]
	    	//directorRating=tokens[2]
	    	//languageRating=tokens[3]
		    //yearRating=token[4];
		    
		    //transform from the type String to  type double
		    double[] subRating=new double[5];
		    for(int i=0;i<5;i++){
		    	subRating[i]=Double.parseDouble(tokens[i]);
		    }
		    
		    //compute the total rating
		    double rating=0.0;
		    for(int i=0;i<5;i++){
		    	rating+=w[i]*subRating[i];			
		    }
		    context.write(key,new DoubleWritable(rating));
    	}
    }
    
    /*
     * define a class UserIdRatingPair as the type of output key in map() function and input key in reduce() function.
     * which implements three abstract methods:readFields();write();compareTo() from interface WritableComparable
	 */
	public static class UserIdRatingPair implements WritableComparable<UserIdRatingPair>{
		private String userId;
		private Double rating;
		
		public String getUserId(){
			return userId;
		}
		public Double getRating(){
			return rating;
		}
		public void setUserId(String userId){
			this.userId=userId;
		}
		public void setRating(double rating){
			this.rating=rating;
		}
		
		public UserIdRatingPair(){			
		}
		 
		@Override
		public void readFields(DataInput in) throws IOException{
			userId=in.readUTF();
			rating=in.readDouble();
		}
		
		@Override
		public void write(DataOutput out) throws IOException{
			out.writeUTF(userId);
			out.writeDouble(rating);
		}
		
		@Override
		/*
		 * this comparator controls the sort order of the keys. 
		 * Here we sort the natural key(userid) first,then sort the secondary key(rating) in the descending order of the rating.
		 */							
		public int compareTo(UserIdRatingPair pair){
			int compareValue=this.userId.compareTo(pair.getUserId());
			if(compareValue==0){
				compareValue=this.rating.compareTo(pair.getRating());
			}
			return -1*compareValue;    //sort descending
		}
		
	}
	
	/*
	 * function:
	 * input data format: <(userid,movieid),rating>
	 * create a composite key of (userid,rating) of UserIdRatingPair class, 
	 * movieid is set to be values.
	 * output data format: <(userid,rating),movieid>
	 */
    public static class Mapper5 extends Mapper<LongWritable, Text, UserIdRatingPair, Text> {      
		@Override
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
			String[] tokens = values.toString().split("[\t|,]");
			//userId=tokens[0]
			//movieId=tokens[1]
			//rating=tokens[2]
			//prepare output key
			UserIdRatingPair compositeKey= new UserIdRatingPair();
			compositeKey.setRating(Double.parseDouble((tokens[2])));
			compositeKey.setUserId(tokens[0]);
			context.write(compositeKey,new Text(tokens[1]));
		}
    }
    
    
    /*
     * partition key by UserId,ensuring that these records with the same UserId will be sent to the same reducer during the shuffling stage.
     */
    public class UserIdRatingPairPartitioner extends Partitioner<UserIdRatingPair,Text>{
    	@Override
    	public int getPartition(UserIdRatingPair pair,Text text,int numberOfPartitions){
    		//make sure that partition are non-negative
    		return Math.abs(pair.getUserId().hashCode()%numberOfPartitions);
    	}
    }
    
    
    /*
     * keys with the same userId are grouped together before calling Reducer5.reduce() function
     */
    public class UserIdRatingPairGroupingComparator extends WritableComparator{
    	public UserIdRatingPairGroupingComparator() {
    		super(UserIdRatingPair.class,true);
    	}
    	
    	@Override
    	public int compare(WritableComparable wc1,WritableComparable wc2){
    		UserIdRatingPair pair1=(UserIdRatingPair)wc1;
    		UserIdRatingPair pair2=(UserIdRatingPair)wc2;
    		return pair1.getUserId().compareTo(pair2.getUserId());
    	}
    }
    
     
    /*
     * function: produce a recommendation list
     * input data format:<(userid,rating),movieid>
     * pick the topK movies for every user
     * output data format:<userid,a sequence of movieid>
     */
    public static class Reducer5 extends Reducer<UserIdRatingPair,Text,Text,Text>{
    	private static int topK=0;
    	
    	@Override
    	protected void setup(Context context) throws IOException, InterruptedException {
	    	//fetch the parameter topK.
    		Configuration conf=context.getConfiguration();
		    topK=Integer.parseInt(conf.get("topK"));
	    }
    	
    	@Override
    	public void reduce(UserIdRatingPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	    
		   //intercept the topK movies as recommendation movies.
    		
    	}
    }
    
   
    public static void runJobs(Map<String, String> path) throws Exception{
    	
    	//compute similarity between movies
    	Configuration conf1 = new Configuration();
	    Job job1 = new Job(conf1, "Compute Similarity");
		job1.setJarByClass(RecommendationSystem.class);
		FileInputFormat.setInputPaths(job1, new Path(path.get("input1")));
		FileOutputFormat.setOutputPath(job1, new Path(path.get("output1")));
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);//
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.waitForCompletion(true);
		
		/*
		//just for testing simlaritymapper3
		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "predicet rating");
		job2.setJarByClass(RecommendationSystem.class);
		FileInputFormat.setInputPaths(job2, new Path(path.get("output1")));
		FileOutputFormat.setOutputPath(job2, new Path(path.get("output2")));
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setMapperClass(SimilarityMapper3.class);
		job2.setReducerClass(Reducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.waitForCompletion(true);
        */
    	
    	/*
		//just for testing ratingsmapper3
		Configuration conf3 = new Configuration();
		Job job3 = new Job(conf3, "predicet rating");
		job3.setJarByClass(RecommendationSystem.class);
		FileInputFormat.setInputPaths(job3, new Path(path.get("input2")));
		FileOutputFormat.setOutputPath(job3, new Path(path.get("output4")));
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		job3.setMapperClass(RatingsMapper3.class);
		job3.setReducerClass(Reducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.waitForCompletion(true);
		*/
		
	    
		//compute subrating for every(userid,movieid)
		Configuration conf3 = new Configuration();
		Job job3 = new Job(conf3, "predict ratings");
		job3.setJarByClass(RecommendationSystem.class);
		MultipleInputs.addInputPath(job3, new Path(path.get("output1")),KeyValueTextInputFormat.class,SimilarityMapper3.class);
		MultipleInputs.addInputPath(job3, new Path(path.get("input2")),TextInputFormat.class,RatingsMapper3.class);
		MultipleOutputs.addNamedOutput(job3, "UnknownRatings", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job3, "KnownRatings", TextOutputFormat.class, Text.class, Text.class);
		job3.setReducerClass(Reducer3.class);
		FileOutputFormat.setOutputPath(job3, new Path(path.get("output3")));
		job3.setMapOutputKeyClass(Text.class);	
		job3.setMapOutputValueClass(Text.class);
		job3.waitForCompletion(true);
    	
    	/*
    	//just for testing reducer4 
		Configuration conf4 = new Configuration();
		Job job4 = new Job(conf4, "predicet rating");
		job4.setJarByClass(RecommendationSystem.class);
		FileInputFormat.setInputPaths(job4, new Path(path.get("output3")));
		FileOutputFormat.setOutputPath(job4, new Path(path.get("output5")));
		job4.setInputFormatClass(KeyValueTextInputFormat.class);
		job4.setMapperClass(Mapper.class);
		job4.setReducerClass(Reducer3.class);
		MultipleOutputs.addNamedOutput(job4, "UnknownRatings", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job4, "KnownRatings", TextOutputFormat.class, Text.class, Text.class);
		job4.setMapOutputKeyClass(Text.class);	
		job4.setMapOutputValueClass(Text.class);
		//job4.setOutputKeyClass(Text.class);
		//job4.setOutputValueClass(Text.class);
		job4.waitForCompletion(true);
		*/
		
		/*
		Configuration conf4 = new Configuration();
		Job job4 = new Job(conf4, "error evaluation");
		job4.setJarByClass(RecommendationSystem.class);
		FileInputFormat.setInputPaths(job4, new Path(path.get("output4")),new Path(path.get("input2")));
		FileOutputFormat.setOutputPath(job4, new Path(path.get("output5")));
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		job4.setMapperClass(Mapper4.class);
		job4.setReducerClass(Reducer4.class);
		job4.setOutputKeyClass(NullWritable.class);
		job4.setOutputValueClass(DoubleWritable.class);
		job4.setNumReduceTasks(1);		//because need to calculate the  global variable "average error".
		job4.waitForCompletion(true);
	    job4.waitForCompletion(true);
	    */

		/*
		//combine the subrating and obtain the total rating with respect to weight
		Configuration conf4 = new Configuration();
		conf4.set("w1",weight[0]);
		conf4.set("w2",weight[1]);
		conf4.set("w3",weight[2]);
		conf4.set("w4",weight[3]);
		conf4.set("w5",weight[4]);
		Job job4 = new Job(conf4, "Compute total rating");
		job4.setJarByClass(RecommendationSystem.class);
		FileInputFormat.setInputPaths(job4, new Path(path.get("output3")));
		FileOutputFormat.setOutputPath(job4, new Path(path.get("output5")));
		job4.setInputFormatClass(KeyValueTextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		job4.setMapperClass(Mapper.class);
		job4.setReducerClass(Reducer4.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(DoubleWritable.class);
		job4.waitForCompletion(true);
		
		//compute recommendation list
		Configuration conf5 = new Configuration();
		conf5.set("topK",""+topK);
		Job job5 = new Job(conf5,"recommendation list");
		job5.setJarByClass(RecommendationSystem.class);
		FileInputFormat.setInputPaths(job5, new Path(path.get("output3")));
		FileOutputFormat.setOutputPath(job5, new Path(path.get("output6")));
		job5.setInputFormatClass(TextInputFormat.class);
		job5.setOutputFormatClass(TextOutputFormat.class);
		job5.setMapperClass(Mapper5.class);
		job5.setReducerClass(Reducer5.class);
		job5.setPartitionerClass(UserIdRatingPairPartitioner.class);
		job5.setGroupingComparatorClass(UserIdRatingPairGroupingComparator.class);
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(Text.class);
		job5.waitForCompletion(true);
		//theLogger.info() collects the log info.
		 */
	}

	/*
	 * drive class
	 * control the parameter setting,input paths and output paths setting,runtime logic.
	 */
	public static void main(String[] args)throws Exception{
		
		if (args.length!=6) {
			System.err.println("Usage: RecommendationSystem <w1> <w2> <w3> <w4> <w5> <topK>");
			System.exit(2);
		}
		
		weight[0]=args[0];
		weight[1]=args[1];
		weight[2]=args[2];
		weight[3]=args[3];
		weight[4]=args[4];
		//threshold=Double.parseDouble(args[4]);
		topK=Integer.parseInt(args[5]);
		
	    
		//set up the all input paths and output paths.
	    String basein="/user/hadoop/recommendationsystem/input/";
	    String baseout="/user/hadoop/recommendationsystem/output/";
	    String input1 = basein+"moviesdata";
		String input2 = basein+"ratingsdata";
		String output1 =baseout+"subsimilarity";
		String output2 =baseout+"similarity";
		String output3 =baseout+"ratings";
		String output4 =baseout+"knownratings";
		String output5 =baseout+"totalratings";
		String output6 =baseout+"recommendationlist";
		
		//prepare the parameter for runJobs()
		Map<String, String> path = new HashMap<String, String>();
		path.put("input1", input1);
		path.put("input2", input2);
		path.put("output1", output1);
		path.put("output2", output2);
		path.put("output3", output3);
		path.put("output4", output4);
		path.put("output5", output5);
		path.put("output3", output3);
		path.put("output6", output6);
		
		runJobs(path);
		/*
	    //check whether error exceeds the predetermined threshold. If it dose,prompt "reset the weight";otherwise proceed to the job5.
		double averageError=0;
		try{
		    Path path= new Path(output5); //can reach the error file,not sure?
		    FileSystem fs=FileSystem.get(new Configuration());
		    Scanner sc = new Scanner(fs.open(path));
		    if(sc.hasNextLine()){
	            averageError = Double.parseDouble(sc.nextLine());
            }
            sc.close();
        }catch(Exception e){System.out.println(e);}	
		
		if(averageError>threshold) {
			System.out.println("averageError= "+averageError+",>"+threshold);
			System.out.println("donot pass the evaluation, please reset the weights");
			System.exit(0);
		}
		else{
			System.out.println("averageError= "+averageError+",<"+threshold);
			System.out.println("congratulations!Pass evaluation,please continue!");
		}
		
		runjob5(path2,topK);
		*/	
		System.exit(0);
	}
}