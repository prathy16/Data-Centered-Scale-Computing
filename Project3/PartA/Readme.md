**Instruction to run the upload.py file**
---
Even though we set the permission of the s3 bucket as public we had to change the line of code in upload.py to:  
```
s3.upload_file("tweets/"+fn, fn,ExtraArgs={'ACL':'public-read'})
```

-----------------------------
1. Downloaded output files from the s3 bucket using the command 
   aws s3 sync s3://output-tweet1-trigger/ partA/Input/
2. Run run.sh
    - merge.py merges all the output files into a single file, mergefile.txt
    - wordcount.py computes the wordcount and writes an output file, worcount.txt
