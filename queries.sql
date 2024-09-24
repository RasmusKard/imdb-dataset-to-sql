CREATE TABLE `dataset_sql`.`content` (
    `tconst` VARCHAR(12) NOT NULL,
    `titleType` ENUM(
        'tvMiniSeries',
        'tvSeries',
        'movie',
        'tvMovie',
        'tvSpecial'
    ) NOT NULL,
    `primaryTitle` VARCHAR(400) NOT NULL,
    `startYear` SMALLINT UNSIGNED NOT NULL,
    `runtimeMinutes` SMALLINT UNSIGNED NULL,
    `averageRating` DECIMAL(3, 1) UNSIGNED NOT NULL,
    `numVotes` MEDIUMINT UNSIGNED NOT NULL,
    PRIMARY KEY (`tconst`),
    UNIQUE INDEX `tconst_UNIQUE` (`tconst` ASC) VISIBLE
);