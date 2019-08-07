const AWS    = require("aws-sdk")
const uuidv1 = require("uuid/v1")
const SNS    = new AWS.SNS()
const DDB    = new AWS.DynamoDB.DocumentClient()

const createJobItem = async (jobId, startedAt, totalPages) => {
  return DDB.put({
    TableName: process.env.JOBS_TABLE_NAME,
    Item: {
      jobId: jobId,
      startedAt: startedAt,
      totalPages: totalPages,
      totalSuccess: 0,
      totalError: 0
    }
  }).promise()
}

const createSNSMessages = (urls, jobId, lighthouseOpts={}) => {
  return urls.map(url => {
    Message: "url ready to process.",
    MessageAttributes: {
      jobId: {
        DataType: "String",
        StringValue: jobId
      },
      url: {
        DateType: "String",
        StringValue: url
      },
      lighthouseOpts: {
        DataType: "String",
        StringValue: JSON.stringify(lighthouseOpts)
      }
    },
    TopicArn: process.env.SNS_TOPIC_ARN
  })
}

module.exports = (evt, ctx, cbk) => {
  const jobId = uuidv1()
  const now   = new Date()
  const urls  = evt.urls

  await createJobItem(jobId, now.toISOStirng(), urls.length)

  const snsMessages = createSNSMessages(urls, jobId, evt.lighthouseOpts)

  await Promise.all(snsMessages.map(msg => SNS.publish(msg).promise()))

  return { jobId }
}
