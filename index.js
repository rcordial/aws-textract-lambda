const { TextractClient, StartDocumentAnalysisCommand, GetDocumentAnalysisCommand } = require('@aws-sdk/client-textract');
const parseOutput = require('./parseOutput');
const timer = ms => new Promise(res => setTimeout(res, ms));
const { Readable } = require('stream');
require('dotenv').config();
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

exports.handler = async (event, context, callback) => {

    // const awsRegion = 'ap-southeast-1';
    // const bucketName = 'textract-glue-demo';
    // const objectKey = 'input/starwars-data-set.pdf';

    const awsRegion = event.Records[0].awsRegion;
    const bucketName = event.Records[0].s3.bucket.name;
    const objectKey = event.Records[0].s3.object.key;   
    
    const textract = new TextractClient({ region: awsRegion });

    const command = new StartDocumentAnalysisCommand({
      DocumentLocation: {
        S3Object: {
          Bucket: bucketName,
          Name: objectKey
        }
      },
      FeatureTypes: ['TABLES']
    });
    
    let textractJob = await textract.send(command);
    
    let documentAnalysisOutput = await getOutput(textractJob.JobId);
    
    let csvOutput = parseOutput(documentAnalysisOutput);


    // Convert the CSV string to a stream
    const stream = Readable.from(Buffer.from(csvOutput));

    const s3Client = new S3Client({
      region: awsRegion, // Replace with your desired region
    });

    outputKey = objectKey.replace('input', 'output').replace('.pdf', '.csv');
    await s3Client.send(new PutObjectCommand({
      Bucket: bucketName,
      Key: outputKey,
      Body: stream,
      ContentType: 'text/csv',
      ContentLength: Buffer.from(csvOutput).length
    }));

    //lambda response
    return {
        statusCode: 200,
        body: `https://${bucketName}.s3.${awsRegion}.amazonaws.com/${outputKey}`
    };
    
};



function getOutput(jobId) {

    return new Promise(async (resolve,reject) => {

        const textract = new TextractClient({ region: process.env.AWS_REGION });
      
        try {
    
            let nextT = null;
    
            var output = {
                NextToken: null,
                JobStatus: 'IN_PROGRESS'
            }
    
            let waitTime = 3000;
            let blocks = [];
    
            console.log(`attempting to retrieve textract job ${jobId}`);

            while(typeof nextT != 'undefined' || output.JobStatus == 'IN_PROGRESS'){
                
                let commander = new GetDocumentAnalysisCommand({
                    JobId: jobId,
                    NextToken: output.NextToken,
                });
    
                output = await textract.send(commander);
    
                if(output.JobStatus == 'IN_PROGRESS'){
                    // if textract job not finished, do exponential backoff
                    await timer(waitTime);
                    waitTime = waitTime*2;
                    continue;
                }
    
                nextT = output.NextToken;
                blocks = blocks.concat(output.Blocks);
    
            }
    

            output.Blocks = blocks;

            console.log(`textract job successful`);
            
            resolve(output);
    
        } catch (err) {
          // Handle error
          reject(err);
        }
        
    });
}

