**Steps**  
1. We created IAM role. Attached Amazon S3 Full access and Amazon CloudWatch Full Access to it.
2. S3 bucket onto which the input must be loaded is created.
3. The permisions for this bucket are set to public.
4. We have to set the s3 trigger for the lambda function. Thus, we attach this bucket.
5. In the lambda function we have the following functions:
    - The trigger is set in such a way that once a file is uploaded to the s3 bucket, it starts to process the information
    - We open the file, read the line, remove stop words, punctuation and write back the processed line to the new file in a different s3  bucket.
