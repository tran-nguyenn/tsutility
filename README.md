# tsutility
Time series calculations including decomposed time series and utility functions like reading sql files and organizing the data for parallelization.


Forecastability XYZ Classification Documentation

Data Science, 2/3/2020


Introduction:
In supply chain, forecastability is the ability to determine how forecastable an item is based on their sales order history. For businesses and demand planners forecastability helps prioritize their efforts based on how easy or difficult an item is to forecast. The Data Science team at Newell Brands has created a forecastable classification method called XYZ that identifies items that are easy to forecast (X), somewhat forecastable (Y), and difficult to forecast (Z).

Methods:
The Data Science team used a combination of coefficient of variation (COV), and time series decomposition to achieve the most interpretable, accurate, and maintainable XYZ classification.

The industry standard to determine forecastability for an item is to use the coefficient of variation, which is the ratio of the standard deviation to the average. This measures the degree of variation from one time series to another, however, the coefficient of variation is known to poorly forecast seasonal data. This is due to the variation seen in seasonal data, however, businesses normally understand their seasonal data quite well and should be labeled as forecastable.

The equation for the coefficient of variation shows us that it only looks at the sales order history with the seasonal elements.

Therefore, we proposed a new forecasting method called the deseasonalized coefficient of variation, which uses the decomposed sales order history and calculates the deseasonalized coefficient of variation by using the trend and the residuals from the decomposition. The equation below shows our calculation for the deseasonalized coefficient of variation.

We calculated the deseasonalized coefficient of variation by first decomposing the sales order history by four components: observed, trend, seasonal, and residuals.
 
The observed, trend, and seasonal are self-explanatory, but the residual are the time series after the trend and seasonal components are removed. Therefore, when we calculate the deseasonalized coefficient of variation we are adding the trend back into the time series, but we’ve removed the seasonal component.
