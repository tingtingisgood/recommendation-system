.PHONY: start openurl uploadmoviedata uploaduserdata uploadratingdata checkoutput1 checkoutput2 checkoutput3 checkoutput4 cleanup1 cleanup2 cleanup3 run
#2 tab!=8 spaces,there must be 2 tabs before every command.

start:
		start-all.sh
		jps
		hadoop dfsadmin -report
openurl:
		xdg-open http://localhost:50070
		xdg-open http://localhost:50030
		
	        

uploadmoviedata:    
		hadoop fs -mkdir /user/hadoop/recommendationsystem/input/moviesdata/
		hadoop fs -mkdir /user/hadoop/recommendationsystem/input/movieiddata/
		hadoop fs -put Movies.txt /user/hadoop/recommendationsystem/input/moviesdata/
		hadoop fs -put MovieId.txt /user/hadoop/recommendationsystem/input/movieiddata/
		hadoop fs -ls -Rl /user/hadoop/recommendationsystem/input/movie*/
uploaduserdata:
		hadoop fs -mkdir /user/hadoop/recommendationsystem/input/useriddata/
		hadoop fs -put UserId.txt /user/hadoop/recommendationsystem/input/useriddata/
		hadoop fs -ls -Rl /user/hadoop/recommendationsystem/input/useriddata
uploadratingdata:
		hadoop fs -mkdir /user/hadoop/recommendationsystem/input/ratingsdata/
		hadoop fs -put Ratings.txt /user/hadoop/recommendationsystem/input/ratingsdata/
		hadoop fs -ls -Rl /user/hadoop/recommendationsystem/input/ratingsdata/ 

checkoutput1:
		hadoop fs -ls /user/hadoop/recommendationsystem/output
		hadoop fs -cat /user/hadoop/recommendationsystem/input/moviesdata/Movies.txt
		hadoop fs -cat /user/hadoop/recommendationsystem/output/subsimilarity/part-r-00000
checkoutput2:
		hadoop fs -ls /user/hadoop/recommendationsystem/output
		hadoop fs -cat /user/hadoop/recommendationsystem/output/subsimilarity/part-r-00000
		
checkoutput3:
		hadoop fs -ls /user/hadoop/recommendationsystem/output
		hadoop fs -cat /user/hadoop/recommendationsystem/input/ratingsdata/Ratings.txt
		hadoop fs -cat /user/hadoop/recommendationsystem/output/ratings/part-r-00000
checkoutput4:
		hadoop fs -ls /user/hadoop/recommendationsystem/output
		hadoop fs -cat /user/hadoop/recommendationsystem/output/ratings/UnknownRatings-r-00000
		hadoop fs -cat /user/hadoop/recommendationsystem/output/ratings/KnownRatings-r-00000

cleanup1:
		hadoop fs -rmr /user/hadoop/recommendationsystem/output/
		
cleanup2:
		hadoop fs -rmr /user/hadoop/recommendationsystem/output/subsimilarity/

cleanup3:
		hadoop fs -rmr /user/hadoop/recommendationsystem/output/ratings/
		


run:    
		hadoop jar MRS.jar mylab0.RecommendationSystem  0.2 0.2 0.2 0.2 0.2 3


