const AWS = require("aws-sdk")
const DDB = new AWS.DynamoDB.DocumentClient()
const S3  = new AWS.S3()

const totalCompletedPages = (image) => {
  return parseInt(image.totalSuccess.N, 10) + parseInt(image.totalError.N, 10)
}

const setJobCompletedAt = (jobId) => {
  const now = new Date()

  const params = {
    TableName: process.env.JOBS_TABLE_NAME,
    Key: {
      jobId: jobId
    },
    UpdateExpression: `SET ${attr} = :val`,
    ExpressionAttributeValues: {
      ":val": now.toISOString()
    },
    ReturnValues: "UPDATED_NEW"
  }

  return DDB.update(params).promise()
}

module.exports = async (evt, ctx, cbk) => {
  const record = evt.Records[0]
  if (record.eventName !== "MODIFY") {
    return Promise.resolve()
  }

  const oldTotalCompletedPages = totalCompletedPages(record.dynamodb.OldImage)
  const newTotalCompletedPages = totalCompletedPages(record.dynamodb.NewImage)
  const totalPages = parseInt(record.dynamodb.NewImage.totalPages.N, 10)

  const jobJustFinished = (oldTotalCompletedPages !== newTotalCompletedPages && newTotalCompletedPages >= totalPages)

  if (!jobJustFinished) {
    return Promise.resolve()
  }

  await setJobCompletedAt(record.dynamodb.NewImage.jobId.S)

  return Promise.resolve()
}
