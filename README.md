# customer-analysis
An analysis of the customer's purchasing behavior given their affinities and geo-spatial coordinates

## Part 1: The Story
Every dataset tells a story. This one did too, it told the story of people who went shopping. A data scientist's job is to help unleash those stories. With this in mind, we embark on a journey to make sense of all these numbers. 

Truth be told: before I received this task, I had zero experience with geospatial data. I had to look into several different things, data structures, infrastructures, frameworks, algorithms, software libraries etc before I got started. I'll try to explain my own journey here. For storing the geospatial data, I looked into geopandas and PostGIS on top of PostGRES. For handling massive amounts of tabular data, I looked into Dask, Vaex and the newly released pandas API on top of pyspark. For handling geospatial operations I looked into shapely. For geo-spatial indexing I looked into Uber's H3 and geopandas' spatial index. For the algorithmic background and the theoretical problem, I looked into the concept of R-Trees and fast point-in-polygon (PIP) queries. 

We have a relatively massive amount of data on our hands and in the first task, we need to find the geographic intersection of a large number of points and a relatively small number of polygons. 

The first solution I came up with was a brute force algorithm using shapely. One can load the wkt data using shapely and then iterate over the stores to see which polygon **contains** that particular point. Is this efficient? No, it's damn slow. Even for the sample data, it took 20 minutes to iterate and find the intersecting points for **one** chunk. It was clear it won't be a solution which works for the full data. 

I had a limited amount of time and a lot of stuff to look into and to try. What I could have done was parallelizing that dummy operation and getting the result for the sample data within several hours so I can answer the other questions. 

I didn't want to do this; I wanted to tackle the full data. I knew I don't have enough time to all the tasks anyway. So what's the point? Of course, it would have been different with a customer waiting on some deadline. But a dummy approach is only sustainable within the very short term and doing analysis on **sample data** is not going to bring the customer anything anyway. I even considered doing this whole thing on a databricks cluster with PySpark. I wasn't sure about uploading the data/using the data over there because of privacy issues. It also wouldn't have been nice to use more computational power with a dummy solution.

So I took on this challenge: to be able to analyze massive amounts of a data type I had never experimented with (i.e. geospatial data) and make it scalable within this short period. And I did that. 

I knew I needed some sort of geo-spatial index to be able to answer those PIP queries in a timely manner. I looked into tools I could find: I considered loading the data in a PostGRES db and doing a spatial join using PostGIS. But then I would lose the power I have with python. I looked into geo-spatial indexing libraries such as Uber's H3. But then I would need to store the data elsewhere and keep track of the index on H3. It was possible, but not the cleanest. 

Enter Geopandas. 

Geopandas offers a neat geospatial index using R-Trees which shorten the time spent on PIP queries signficantly. It offers data storage, the same pandas API for joins (similar to PostGIS). Too good to be true. I went for it, paralellized the whole operation with multiprocessing. The whole 1GB with 57 data chunks took around 4000s to run all the spatial joins, stack the results from each chunk on top of each other and writing all of it back to disk on my 2,6GHz 6-corei7 MacBook Pro with 8 processes. It could have been faster, but it's definitely manageable. 

## Part 2: Project Structure
The code comes with three modules: one "load_data.py" for... well, obviously loading the data, then "combine_data.py" for bringing different data sources together via geo-spatial joins and finally "visualization.py" for visualizing. It is assumed that the folder "train_data" is located in the root of the project. It goes without saying that I couldn't have uploaded the whole data here, but for the code to work, that data must be there. Necessary libraries are listed in "requirements.txt"; plots and visualizations are located in the "plots" folder. The results of unifying the data are located in the "derived_data" directory.

## Part 3: Analytics & Visualizations
In "combine_data.py" the analytics metrics have been computed. I didn't get to finish the whole thing, because it was simply a lot to do. However, I did visualize what I had to and I can elaborate verbally on the sort of predictive models which might make sense. 

For one thing, it was impossible to get to the data scientists' section without completing the initial section. We are in the end interested in knowing if we can predict a user's shopping behavior given his affinities and past shopping behavior so as to send him/her the best subset of advertisements. In this light, following statistial questions are important:

1) The covariance between population groups and shopping at a certain store.
2) Consistency/covariance inside a group in shopping activity. How similar does a certain population group behave?
3) Seasonality in each store: are there specific times a store is particularly popular among any group in the population?
4) Given a certain advertising campaign at a point in time, does customer behavior change? (A/B testing)

The problem of predicting whether or not a customer will go to a certain store can be mathematically modelled by formulating it as a matrix factorization. We are in fact interested in the latent features which form the customer behavior. Given every user and their affinity, also given every user and their past behavior, we are interested in the matrix which relates each store to an affinity. This is one way to formulate this problem; other formulations (e.g. a probabilistic formulations) are also possible and highly interesting. Possible sorts of factorizations to explore would be SVD, NMF and LDA.
