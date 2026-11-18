# Resumen EDA (generado automáticamente)

## Columnas comunes a todos los años
['Generosity']

## Columnas únicas por año
- 2015: 12 columnas; únicas: ['Standard Error']
- 2016: 13 columnas; únicas: ['Lower Confidence Interval', 'Upper Confidence Interval']
- 2017: 12 columnas; únicas: ['Dystopia.Residual', 'Economy..GDP.per.Capita.', 'Happiness.Rank', 'Happiness.Score', 'Health..Life.Expectancy.', 'Trust..Government.Corruption.', 'Whisker.high', 'Whisker.low']
- 2018: 9 columnas; únicas: []
- 2019: 9 columnas; únicas: []

## Registros por año
year
2015    158
2016    157
2017    155
2018    156
2019    156

## Faltantes (dataset de modelado)
corruption    1

## Atípicos (IQR, solo marcados)
       variable  n_atipicos  pct
happiness_score           0 0.00
 gdp_per_capita           0 0.00
 social_support          10 1.28
life_expectancy           0 0.00
        freedom           0 0.00
     corruption          67 8.57
     generosity          17 2.17

## Estadísticos descriptivos
                 count   mean    std    min    25%    50%    75%    max
happiness_score  782.0  5.379  1.127  2.693  4.510  5.322  6.190  7.769
gdp_per_capita   782.0  0.916  0.407  0.000  0.606  0.982  1.236  2.096
social_support   782.0  1.078  0.330  0.000  0.869  1.125  1.327  1.644
life_expectancy  782.0  0.612  0.248  0.000  0.440  0.647  0.808  1.141
freedom          782.0  0.411  0.153  0.000  0.310  0.431  0.531  0.724
corruption       781.0  0.125  0.106  0.000  0.054  0.091  0.156  0.552
generosity       782.0  0.219  0.122  0.000  0.130  0.202  0.279  0.838

## Media por año
      happiness_score  gdp_per_capita  social_support  life_expectancy  freedom  corruption  generosity
year                                                                                                   
2015            5.376           0.846           0.991            0.630    0.429       0.143       0.237
2016            5.382           0.954           0.794            0.558    0.371       0.138       0.243
2017            5.354           0.985           1.189            0.551    0.409       0.123       0.247
2018            5.376           0.891           1.213            0.597    0.455       0.112       0.181
2019            5.407           0.905           1.209            0.725    0.393       0.111       0.185

## Correlación con el score
gdp_per_capita     0.789
life_expectancy    0.742
social_support     0.649
freedom            0.551
corruption         0.398
generosity         0.138