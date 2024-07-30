# Used_Car_Price_EDA

## Introduction
There is a huge demand for used cars in the Indian Market today. As sales of new cars have 
slowed down in the recent past, the pre-owned car market has continued to grow over the 
past years and is larger than the new car market now. Cars4U is a budding tech start-up that 
aims to find foot holes in this market. In 2018-19, while new car sales were recorded at 3.6 
million units, around 4 million second-hand cars were bought and sold. There is a slowdown 
in new car sales and that could mean that the demand is shifting towards the pre-owned 
market. In fact, some car sellers replace their old cars with pre-owned cars instead of buying 
new ones. Unlike new cars, where price and supply are fairly deterministic and managed by 
OEMs (Original Equipment Manufacturer / except for dealership level discounts which come 
into play only in the last stage of the customer journey), used cars are very different beasts 
with huge uncertainty in both pricing and supply. Keeping this in mind, the pricing scheme of 
these used cars becomes important in order to grow in the market. We have to come up with 
a pricing model that can effectively predict the price of used cars and can help the business 
in devising profitable strategies using differential pricing.

## Problem Statement
Identify the factors that affect a second-hand car's value, leading us to create a car price 
prediction model in near future, which may help the buyers to learn the actual market value 
of a car before buying or selling. Before we create our own car price prediction model, let's 
understand on what really affects a car's price.
Questions we will be answering here before any prediction model: 
* Does various predicating factors affect the price of the used car .?
* What all independent variables effect the pricing of used cars?
* Does name of a car have any effect on pricing of car.?
* How does type of Transmission effect pricing?
* Does Location in which the car being sold has any effect on the price?
* Do kilometres Driven; Year of manufacturing have negative correlation with price of 
the car?
* Does Mileage, Engine and Power have any effect on the pricing of the car?
* How does number of seats, Fuel type effect the pricing.

## EDA Process Step involve
* Data Set
* Problem
* Libraries
* Read and Understand Data
* Data Preprocessing
* Basic EDA
* Handling Missing Value
* Exploratory Data Analysis
* Insights based on EDA
* Model Building

Data Set

* S.No. : Serial Number
* Name : Name of the car which includes Brand name and Model name
* Location : The location in which the car is being sold or is available for purchase Cities<b
r>
* Year : Manufacturing year of the car
* Kilometers_driven : The total kilometers driven in the car by the previous owner(s) in KM.
* Fuel_Type : The type of fuel used by the car. (Petrol, Diesel, Electric, CNG, LPG)
* Transmission : The type of transmission used by the car. (Automatic / Manual)
* Owner : Type of ownership
* Mileage : The standard mileage offered by the car company in kmpl or km/kg
* Engine : The displacement volume of the engine in CC.
* Power : The maximum power of the engine in bhp.
* Seats : The number of seats in the car.
* New_Price : The price of a new car of the same model in INR Lakhs.(1 Lakh = 100, 000)
* Price : The price of the used car in INR Lakhs (1 Lakh = 100, 000)
