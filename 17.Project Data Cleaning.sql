select * 
from layoffs;

create table layoffs_staging           # in database
like layoffs;


select *
from layoffs_staging;                  # we have all columns empty

insert layoffs_staging                 # get data    // make a copy
select *
from layoffs;

# step 1

select *,                                                          # create row number and match it with all columns
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`,      # date: because its a keyword
                  stage, country, funds_raised_millions) as row_num
from layoffs_staging;

with duplicate_cte as                                         # put the row number in cte
(
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`,
                  stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select * 
from duplicate_cte
where row_num > 1;                         # filter dublicates on row number if they > 1  ----> results here(>=2) are duplicates and we wont to remove it


select *                                   # cheack
from layoffs_staging
where company = 'casper';


create table `layoffs_staging2` (               # create a new table containe dublicates(>=2) and deleting it
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2                    # we have all columns(duplicates) empty
where row_num > 1;                       # filtering   //       result here is duplicates and we wont to remove it 

insert into layoffs_staging2             # fill the new table with columns
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`,
                  stage, country, funds_raised_millions) as row_num
from layoffs_staging;

delete
from layoffs_staging2
where row_num >1;

select *
from layoffs_staging2;             # no duplicates


# step 2

select company, trim(company)                  # scan if we have an issue
from layoffs_staging2;

update layoffs_staging2b                       # fixed it
set company = trim(company);


select distinct location                        # scan if we have an issue
from layoffs_staging2
order by 1;

SELECT distinct industry                        # search or scan if we have an issue
FROM layoffs_staging2;

UPDATE layoffs_staging2                         # define it and fixed
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


select distinct country                        # scan if we have an issue
from layoffs_staging2
order by 1;

select distinct country, trim(trailing '.' from country)                      
from layoffs_staging2
order by 1;

UPDATE layoffs_staging2
SET country = trim(trailing '.' from country)
WHERE country LIKE 'United States%';


select distinct `date`,                        
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

select `date` 
from layoffs_staging2;

alter table layoffs_staging2                                       # change datatype of table
modify column `date` date;


# step 3

select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


# step 4

select * 
from layoffs_staging2
where industry is null
or industry = '';

select *                                 # populate data
from layoffs_staging2
where company = 'Airbnb';


select t1.industry, t2.industry                        # fill or populate the blank row in column industry   of data not blank
from layoffs_staging2 t1
join layoffs_staging2 t2
    on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2
set industry = null
where industry = '';

update layoffs_staging2 t1
join layoffs_staging2 t2
    on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;




delete                                         # remove columns or rows
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;

select *                                          # final clean date
from layoffs_staging2;





-- # Explore Data --


select * 
from layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select min(`date`), max(`date`)
from layoffs_staging2;

select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

select * 
from layoffs_staging2;

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 desc;

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 1 desc;

select company, avg(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select substring(`date`,6,2) as `month`, sum(total_laid_off)                   # rolling total
from layoffs_staging2
group by `month`;

select substring(`date`,1,7) as `month`, sum(total_laid_off)                   
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc;

with rolling_total as
(
select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_off               
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)
select `month`, total_off, sum(total_off) over(order by `month`) as rolling_total
from rolling_total;


select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
order by company asc;

select company, year(`date`), sum(total_laid_off)               # rank
from layoffs_staging2
group by company, year(`date`)
order by 3 desc;

with company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)              
from layoffs_staging2
group by company, year(`date`)
),
company_year_rank as
(
select *, dense_rank() over (partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null
)
select *                            # filter rank
from company_year_rank
where ranking <= 5;




