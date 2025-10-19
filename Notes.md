<u>_**Module 5: Data Pipeline Basics**_</u>

**(5.1) What is data pipeline?**
- Data pipeline is the ability to have flow of information that can then be translated into insight and knowledge so that the business can make a better decision


**(5.2) How do they work?**
- Cron - the very first technology really need to learn to really understand data pipeline
- Cron is based on your computer’s clock, and your computer’s clock is based on a number of seconds since January 1st 1970 (called as EPOCH)
- Cron is the ability to schedule things based on the number of seconds between tasks
- Cron is the technology that allows data pipelines to schedule and this causes the magic to happen, this is the automation part.
- Cron is the fundamental technology under all pipeline tools like Airflow and Dagster and Databricks workflow


**(5.3) Types of pipelines**
- Regularly scheduled pipelines (Cron)
- Real-time pipeline (which looking for events all the time to do the action)
- On-demand pipeline


<u>_**Module 6: Data Modeling Basics**_</u>

- Data modeling is one of the most important skills that data engineers have to learn.
- At the end of the data modeling is, structure of the data that we create for businesses because 
    - the asset that we produce for business is data to make better decisions 
- Data modeling has a bunch of different components to it
    1. Conceptual data modeling (Highest level)
        - Which is the highest level of thinking about it is like 
            - what data do we need
            - what data does the business desire
            - what data do we have access to
            - what are the sources
            - Where can we find this data
            - can we like think about all the relationships of this data like at a very high level
        - Because sometimes you might want data in a business but it's actually unfeasible to get it 
        - or it's going to take months and months, and it's not ROI positive (Because the time it would take to get the data is more than the value you can get out of it)
        
        - So this area of data modeling is very very important because if you get this wrong you can end up something a lot of extra time on the data model for things that don't matter that much

    2. Logical data modeling (Next layer down)
        - Where you start talking about like facts and dimensions and how they're related 
            - like so facts are events like you think of that like
                - I click the login button
                - I purchase a product, etc.
                - all of these are like events and actions that you can take on a website and these are gonna be your facts because they can't really change after I've logged in on Facebook at 6:04 PM, I can't go back and change it.
            - then you have dimensions 
                - which are gonna be like your nouns or like your actors in this space
                - you have things like users or listings or posts or devices these are all like things that bring context
                - like I'm a user in China who clicks an ad 
                    - Here, user part is dimension
                    - and clicks part is facts

    3. Physical data modeling (Final layer)
        - Where people oftentimes they think the physical layer is the only part of data modeling when it's probably the least important in some regards
        - but it also becomes real, reality, it's physical data now
        - this is where you start looking at the schema, 
            - what are the columns of your data, 
            - what are the data types of your data, 
            - how are you storing this data,
            - how can you compress the data to make it smaller