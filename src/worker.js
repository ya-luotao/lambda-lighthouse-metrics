const createLighthouse = require("./lib/create-lighthouse.js")
const fs               = require("fs")
const AWS              = require("aws-sdk")
const DDB              = new AWS.DynamoDb.DocumentClient()
const S3               = new AWS.S3()

const updateToS3(key, contentType, body) => {
  return s3.upload({
    Bucket: process.env.BUCKET,
    Key: key,
    Body: body,
    ContentType: contentType
  }).promise()
}

const updateJob(jobId, jobIncreAttr, runId, runUrl, runError) => {
  const updateJob = {
    TableName: process.env.JOBS_TABLE_NAME,
    Key: {
      jobId: jobId
    },
    UpdateExpression: `SET ${jobIncreAttr} = ${jobIncreAttr} + :val`,
    ExpressionAttributeValues: {
      ":val": 1
    }
  }

  await DDB.update(updateJob).promise()

  const newRun = {
    TableName: process.env.RUNS_TABLE_NAME,
    Item: {
      jobId: jobId,
      runId: runId
    }
  }

  if (runError) {
    newRun.Item.Error = runError
  }

  return DDB.put(newRun).promise()
}

const s3Key = (jobId, runId, format) => {
  return `raw_reports/${format}/jobs/${jobId}/runs/${runId}.${format}`
}

const doesRunItemExist = (runId, consistentRead=false) => {
  const params = {
    TableName: process.env.RUNS_TABLE_NAME,
    ConsistentRead: consistentRead,
    Key: {
      runId: runId
    }
  }

  let exists = false
  const result = await DDB.get(params).promise()
  if (result.Item !== undefined && result.Item !== null) {
    exists = true
  }

  return Promise.resolve(exists)
}

module.exports = (evt, ctx, cbk) => {
  const record          = evt.Records[0]
  const topicArn        = record.Sns.TopicArn
  const snsMessage      = record.Sns.Message
  const snsMessageAttrs = record.Sns.MessageAttributes

  if (topicArn == process.env.DLQ_ARN) {
    const originalMessage = JSON.parse(snsMessage)
    const originalRecord = originalMessage.Records[0]
    console.log(
      "processing record from DLQ; original record:",
      JSON.stringify(originalRecord)
    )

    let jobId
    try {
      jobId = originalRecord.Sns.MessageAttributes.jobId.Value
    } catch (err) {
      return Promise.resolve()
    }

    return await updateJob(jobId, "totalError", originalRecord.Sns.MessageId, originalRecord.Sns.MessageAttributes.url.Value, `ended up in dlq: ${JSON.stringify(snsMessageAttrs.ErrorMessage.Value)}`)

  }

  const jobId          = snsMessageAttrs.jobId.Value
  const lighthouseOpts = JSON.parse(snsMessageAttrs.LighthouseOpts.Value)
  const runId          = record.Sns.MessageId
  const url            = snsMessageAttrs.url.value
  const jsonS3Key      = s3Key(jobId, runId, "json")
  const htmlS3Key      = s3Key(jobId, runId, "html")

  let exist = await doesRunItemExist(runId)

  if (exist) {
    return Promise.resolve()
  }

  const { chrome, start } = await createLighthouse(url, {
    ...lighthouseOpts,
    output: ["json", "html"]
  })
  const results = await start()
  const [jsonReport, htmlReport] = results.report

  exist = await doesRunItemExist(runId, true)

  if (exist) {
    return chrome.kill()
  }

  try {
    await updateToS3(jsonS3Key, jsonReport, "application/json")
    await updateToS3(htmlS3Key, htmlReport, "text/html")
  } catch (err) {
    console.log("error uploading reports to s3:", err)
  }

  await updateJob(jobId, "totalSuccess", runId, url)

  return chrome.kill()
}
