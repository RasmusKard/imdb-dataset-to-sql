**Setting up database:**

_TITLE TABLE_

tconst (VARCHAR) (Unique index) - alphanumeric unique identifier of the title
titleType (ENUM) – the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)
primaryTitle (VARCHAR) – the more popular title / the title used by the filmmakers on promotional materials at the point of release
startYear (SMALLINT (UNSIGNED)) – represents the release year of a title. In the case of TV Series, it is the series start year
runtimeMinutes (SMALLINT (UNSIGNED)) – primary runtime of the title, in minutes
averageRating (DECIMAL(3, 1)) – weighted average of all the individual user ratings
numVotes (MEDIUMINT (UNSIGNED)) - number of votes the title has received

1. Change '/N' values to NULL
1. Drop rows where genres = NULL
1. Drop rows where titletype = 'tvEpisode' OR 'videoGame'
1. Drop rows where isAdult = 1
1. Drop columns:

-   originalTitle
-   isAdult
-   endYear

1. Where startYear = NULL change to 0
1. Join title table and ratings on tconst

_GENRES TABLE_

tconst (VARCHAR) (Unique index) - alphanumeric unique identifier of the title
genre (ENUM) – one genre per row

1. Use title file data
1. Split genres by comma and add them to new table
1. Link genres table and title table by tconst

**NOTES**

Movie = ('movie', 'tvMovie', 'tvSpecial', 'short', 'video')

TV Show = ('tvMiniSeries', 'tvSeries', 'tvShort')
