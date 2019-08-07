const chromePath     = require.resolve("@serverless-chrome/lambda/dist/headless-chromium")
const chromeLauncher = require("chrome-launcher")
const lighthouse     = require("lighthouse")

const defaultFlags   = [
  "--headless",
  "--disable-dev-shm-usage",
  "--disable-gpu",
  "--no-zygote",
  "--single-process",
  "--hide-scrollbars"
]

module.exports = (url, opts={}, config) => {
  opts.output = opts.output || "html"

  const log = opts.level ? require("lighthouse-logger") : null
  if (log) {
    log.setLevel(opts.logLevel)
  }
  const chromeFlags = opts.chromeFlags || defaultFlags

  return chromeLauncher.launch(
    {
      chromeFlags,
      chromePath
    }
  ).then(chrome => {
    opts.port = chrome.port

    return {
      chrome,
      log,
      start() {
        return lighthouse(url, opts, config)
      }
    }
  })
}
