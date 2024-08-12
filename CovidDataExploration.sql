/*
Covid-19 Data Exploration 

Skills used: Joins, CTEs, Temp Tables, Window Functions, Aggregate Functions, Creating Views, Converting Data Types

This query performs various analyses on COVID-19 data, including infection rates, death percentages, vaccination progress, and continental comparisons.
*/

-- Initial Data Exploration
Select Location, date, total_cases, new_cases, total_deaths, population
From PortfolioProject..CovidDeaths
Where continent is not null 
Order by 1,2;

-- Total Cases vs. Total Deaths Analysis
Select Location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as DeathPercentage
From PortfolioProject..CovidDeaths
Where location = 'India'
and continent is not null 
Order by 1,2;

-- Infection Rate Analysis
Select Location, date, Population, total_cases, (total_cases/population)*100 as PercentPopulationInfected
From PortfolioProject..CovidDeaths
Order by 1,2;

-- Countries with Highest Infection Rate
Select Location, Population, MAX(total_cases) as HighestInfectionCount, Max((total_cases/population))*100 as PercentPopulationInfected
From PortfolioProject..CovidDeaths
Group by Location, Population
Order by PercentPopulationInfected desc;

-- Highest Death Count by Country
Select Location, MAX(cast(Total_deaths as int)) as TotalDeathCount
From PortfolioProject..CovidDeaths
Where continent is not null 
Group by Location
Order by TotalDeathCount desc;

-- Death Count by Continent
Select dea.continent, MAX(cast(Total_deaths as int)) as TotalDeathCount
From PortfolioProject..CovidDeaths dea
Where dea.continent is not null 
Group by dea.continent
Order by TotalDeathCount desc;

-- Global Summary
Select SUM(cast(new_cases as int)) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/SUM(cast(New_Cases as int))*100 as DeathPercentage
From PortfolioProject..CovidDeaths
where continent is not null 
order by 1,2;

-- Vaccination Progress
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
From PortfolioProject..CovidDeaths dea
Join PortfolioProject..CovidVaccinations vac
On dea.location = vac.location
and dea.date = vac.date
where dea.continent is not null 
order by 2,3;

-- Using CTE for Population vs. Vaccination Analysis
With PopvsVac (Continent, Location, Date, Population, New_Vaccinations, RollingPeopleVaccinated)
as
(
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
From PortfolioProject..CovidDeaths dea
Join PortfolioProject..CovidVaccinations vac
On dea.location = vac.location
and dea.date = vac.date
where dea.continent is not null 
)
Select *, (RollingPeopleVaccinated/Population)*100 as VaccinationPercentage
From PopvsVac;

-- Using a Temp Table for Population vs. Vaccination Analysis
DROP Table if exists #PercentPopulationVaccinated;
Create Table #PercentPopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population numeric,
New_vaccinations numeric,
RollingPeopleVaccinated numeric
);

Insert into #PercentPopulationVaccinated
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
From PortfolioProject..CovidDeaths dea
Join PortfolioProject..CovidVaccinations vac
On dea.location = vac.location
and dea.date = vac.date;

Select *, (RollingPeopleVaccinated/Population)*100 as VaccinationPercentage
From #PercentPopulationVaccinated;

-- Top 10 Countries by Total Cases
SELECT TOP 10 location, MAX(cast(total_cases as int)) as TotalCases
FROM PortfolioProject..CovidDeaths
WHERE continent is not null
GROUP BY location
ORDER BY TotalCases DESC;

-- Daily Increase in Cases by Country
SELECT location, date, cast(total_cases as int) as total_cases,
       cast(total_cases as int) - LAG(cast(total_cases as int), 1) OVER (PARTITION BY location ORDER BY date) as DailyIncrease
FROM PortfolioProject..CovidDeaths
WHERE continent is not null
ORDER BY location, date;

-- Countries with Zero New Cases
SELECT location, date, new_cases
FROM PortfolioProject..CovidDeaths
WHERE new_cases = 0 AND continent is not null
ORDER BY location, date;

-- Highest Death Rate by Country
SELECT location, MAX(cast(total_deaths as int)) as TotalDeaths, MAX(cast(total_cases as int)) as TotalCases,
       (MAX(cast(total_deaths as int)) * 1.0 / MAX(cast(total_cases as int))) * 100 as DeathRate
FROM PortfolioProject..CovidDeaths
WHERE continent is not null
GROUP BY location
HAVING MAX(cast(total_cases as int)) > 0
ORDER BY DeathRate DESC;

-- Vaccination Progress Over Time in India
SELECT date, SUM(CONVERT(int, new_vaccinations)) OVER (ORDER BY date) as CumulativeVaccinations
FROM PortfolioProject..CovidVaccinations
WHERE location = 'India'
ORDER BY date;

-- Continental Comparisons
SELECT dea.continent, 
       SUM(cast(total_cases as bigint)) as TotalCases, 
       SUM(cast(total_deaths as bigint)) as TotalDeaths, 
       SUM(CONVERT(int, cv.new_vaccinations)) as TotalVaccinations
FROM PortfolioProject..CovidDeaths dea
JOIN PortfolioProject..CovidVaccinations cv
    ON dea.location = cv.location AND dea.date = cv.date
WHERE dea.continent is not null
GROUP BY dea.continent
ORDER BY TotalCases DESC;

-- Average Daily New Cases and Deaths by Country
SELECT location,
       AVG(cast(new_cases as int)) as AvgDailyNewCases,
       AVG(cast(new_deaths as int)) as AvgDailyNewDeaths
FROM PortfolioProject..CovidDeaths
WHERE continent is not null
GROUP BY location
ORDER BY AvgDailyNewCases DESC;

-- Top 5 Countries with the Fastest Vaccination Rate
SELECT TOP 5 location, AVG(CONVERT(int, new_vaccinations)) as AvgDailyVaccinations
FROM PortfolioProject..CovidVaccinations
WHERE continent is not null
GROUP BY location
ORDER BY AvgDailyVaccinations DESC;

-- Total Vaccinations by Date
SELECT date, SUM(CONVERT(int, new_vaccinations)) as TotalVaccinations
FROM PortfolioProject..CovidVaccinations
WHERE continent is not null
GROUP BY date
ORDER BY date;

-- Consistency in Daily New Cases
SELECT location, STDEV(cast(new_cases as int)) as DailyCasesStdDev
FROM PortfolioProject..CovidDeaths
WHERE continent is not null
GROUP BY location
ORDER BY DailyCasesStdDev ASC;

-- Creating a View for Vaccination Data
Create View PercentPopulationVaccinated as
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
From PortfolioProject..CovidDeaths dea
Join PortfolioProject..CovidVaccinations vac
On dea.location = vac.location
and dea.date = vac.date
where dea.continent is not null;
