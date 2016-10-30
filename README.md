# recommendation-system(CF+content)
recommend K movies for every user based on their watching hostory.

#job's function clarification:

There are now four jobs here.

job1:compute the similarity respectively for movie genre,actor,director,language,year

job2:compute the ratings respectively for movie genre,actor,director,language,year

job4:compute the total ratings with respect to  weight.

job5:generate the recommendation list.

#input/output data clarification:
input data:

    /user/hadoop/recommendationsystem/input/movieiddata/MovieId.txt
    /user/hadoop/recommendationsystem/input/moviesdata/Movies.txt
    /user/hadoop/recommendationsystem/input/useriddata/UserId.txt
    /user/hadoop/recommendationsystem/input/ratingsdata/Ratings.txt
    
job1 output data:

    /user/hadoop/recommendationsystem/output/subsimilarity/part-r-00000
    
job3 output data:

    /user/hadoop/recommendationsystem/output/ratings/UnknownRatings-r-00000
    /user/hadoop/recommendationsystem/output/ratings/KnownRatings-r-00000
    
job4 output data:

    /user/hadoop/recommendationsystem/output/totalratings/part-r-00000
    
job5 output data:

    /user/hadoop/recommendationsystem/output/recommendationlist/part-r-00000
