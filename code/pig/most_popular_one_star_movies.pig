ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|') AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

nameLookup = FOREACH metadata GENERATE movieID, movieTitle, ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;

ratingsByMovie = GROUP ratings BY movieID;

avgCntRatings = FOREACH ratingsByMovie GENERATE group AS movieID, AVG(ratings.rating) AS avgRating, COUNT(ratings.rating) AS cntRating;

oneStarMovies = FILTER avgCntRatings BY avgRating < 2.0;

oneStarWithData = JOIN oneStarMovies BY movieID, nameLookup BY movieID;

finalResults = FOREACH oneStarWithData GENERATE nameLookup::movieTitle AS movieTitle, oneStarMovies::avgRating AS avgRating, oneStarMovies::cntRating AS cntRating;

popOneStarMovies = ORDER finalResults BY cntRating DESC;

DUMP popOneStarMovies;