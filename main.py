# This is a sample Python script.

# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import app

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    str_execution_mode = "Local"
    # str_execution_mode = "S3"
    
    pais = "nicaragua"
    
    event_data = {"S3Bucket": "s3-calden-scraping",
                  "ExecutionMode": str_execution_mode}
    
    return_value = app.handler(event_data, "", pais)
    print(return_value)
    
    
    


