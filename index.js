const express = require("express");
const cors = require("cors");
require("dotenv").config();
const ObjectID = require("mongodb").ObjectID;
const bodyParser = require("body-parser");
const _ = require("lodash");
const path = require("path");

const app = express();
app.use(express.static("../bandhan_retailer_client/build"));
app.use(cors());
app.use(bodyParser.urlencoded({ extended: false, limit: "5000mb" }));
app.use(bodyParser.json({ limit: "5000mb" }));
const port = 6011;

const MongoClient = require("mongodb").MongoClient;
// const MongoClient = require("mongodb").MongoClient;
// const uri = "mongodb://127.0.0.1:27017/retailer_bandhan";
const uri =
  "mongodb+srv://aktcl:01939773554op5t@cluster0.9akoo.mongodb.net/retailer_bandhan?retryWrites=true&w=majority";
const client = new MongoClient(uri, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});
client.connect((err) => {
  const userCollection = client.db("retailer_bandhan").collection("users");
  const adminCollection = client.db("retailer_bandhan").collection("admins");
  const leadsCollection = client.db("retailer_bandhan").collection("leads");
  const statusCollection = client.db("retailer_bandhan").collection("status");
  console.log("user Connection");
  app.get("/agent", (req, res) => {
    const email = req.query.email;
    console.log(email);
    userCollection.find({ email: email }).toArray((err, agents) => {
      console.log(agents[0]);
      res.send(agents[0]);
    });
  });
  app.get("/admin", (req, res) => {
    const email = req.query.email;
    console.log(email);
    adminCollection.find({ email: email }).toArray((err, admins) => {
      console.log(admins[0]);
      res.send(admins[0]);
    });
  });

  app.get("/dMatched", (req, res) => {
    const details = req.query;
    const for_d = "d";
    leadsCollection.find({ for_d: for_d }).toArray((err, d) => {
      const Consumer_No = parseInt(details.Consumer_No);
      const dNumber = d.find(
        (dOut) =>
          dOut.Consumer_No === Consumer_No && dOut.week_name === details.status
      );
      res.send(dNumber);
    });
  });
  app.patch("/answers/:id", (req, res) => {
    const answers = req.body;
    console.log(answers);
    const id = ObjectID(req.params.id);
    leadsCollection
      .updateOne(
        { _id: id },
        {
          $set: {
            answer1_w1: answers.ans1_w1,
            answer2_w1: answers.ans2_w1,
            answer3_w1: answers.ans3_w1,

            answer1_w2: answers.ans1_w2,
            answer2_w2: answers.ans2_w2,
            answer3_w2: answers.ans3_w2,

            answer1_w3: answers.ans1_w3,
            answer2_w3: answers.ans2_w3,
            answer3_w3: answers.ans3_w3,

            answer1_w4: answers.ans1_w4,
            answer2_w4: answers.ans2_w4,
            answer3_w4: answers.ans3_w4,

            newRetailerName_w1: answers.newRetailerName_w1,
            newRetailerName_w2: answers.newRetailerName_w2,
            newRetailerName_w3: answers.newRetailerName_w3,
            newRetailerName_w4: answers.newRetailerName_w4,

            answer4: answers.ans4,
            answer4dot1: answers.ans4dot1,
            answer5: answers.ans5,
            answer5dot1: answers.ans5dot1,
            answer6: answers.ans6,
            answer6dot1: answers.ans6dot1,
            answer7: answers.ans7,
            answer7dot1: answers.ans7dot1,
            agentID: answers.agentID,
            callDate: answers.callDate,
            callTime: answers.callTime,
          },
        }
      )
      .then((result) => {
        console.log(result);
      });
  });
  app.get("/reports", (req, res) => {
    leadsCollection.find({}).toArray((err, reports) => {
      res.send(reports);
    });
  });
  app.get("/qc/:number", (req, res) => {
    const number = req.params.number;
    leadsCollection.find({ Consumer_No: number }).toArray((err, qcs) => {
      console.log(qcs);
      res.send(qcs);
    });
  });
  app.get("/update/:id", (req, res) => {
    const id = req.params.id;
    console.log(id);
    leadsCollection
      .find({ _id: ObjectID(req.params.id) })
      .toArray((err, update) => {
        console.log(update);
        res.send(update);
      });
  });
  app.delete("/deleteAll", (req, res) => {
    leadsCollection.deleteMany({}).then((result) => {
      console.log(result);
      res.send(result.deletedCount > 0);
    });
  });
  app.patch("/finalUpdate/:id", (req, res) => {
    const id = ObjectID(req.params.id);
    const update = req.body;
    console.log(id);
    console.log(update);
    leadsCollection
      .updateOne(
        { _id: id },
        {
          $set: {
            answer1: update.answer1,
            answer2: update.answer2,
            answer3: update.answer3,
            answer4: update.answer4,
            answer5: update.answer5,
            answer6: update.answer6,
            answer7: update.answer7,
            answer8: update.answer8,
            qcChecked: update.qcChecked,
            remarks: update.remarks,
            rating: update.rating,
            qcDate: update.qcDate,
            qcTime: update.qcTime,
          },
        }
      )
      .then((result) => {
        console.log(result);
        res.send(result.modifiedCount > 0);
      });
  });
  app.get("/finalReportLead", (req, res) => {
    reportsCollection.find({}).toArray((err, finalLeads) => {
      console.log(finalLeads);
      res.send(finalLeads);
    });
  });
  app.post("/uploadLead", (req, res) => {
    const leadData = req.body;
    console.log(leadData);
    leadsCollection.insertMany(leadData).then((result) => {
      res.send(result.insertedCount > 0);
    });
  });
  app.post("/adminSignUp", (req, res) => {
    const admin = req.body;
    adminCollection.insertOne(admin).then((result) => {
      res.send(result.insertedCount > 0);
    });
  });
  app.post("/reportsData", (req, res) => {
    const detailsReports = req.body;
    detailsReportCollection.insertMany(detailsReports).then((result) => {
      res.send(result.insertedCount > 0);
    });
  });
  app.get("/reportDates", async (req, res) => {
    async function analyzeData() {
      let result = [];
      try {
        let data = await leadsCollection.find({}).toArray();
        let dates = _.groupBy(JSON.parse(JSON.stringify(data)), function (d) {
          return d.data_date;
        });
        for (date in dates) {
          result.push({
            date: date,
          });
        }
      } catch (e) {
        console.log(e.message);
      }
      res.send(result);
    }
    analyzeData();
  });
  app.get("/prepareByDate", (req, res) => {
    let pDate = req.query;
    console.log(pDate.date);
    leadsCollection.find({ data_date: pDate?.date }).toArray((err, result) => {
      res.send(result);
    });
  });
  app.delete("/deleteByDate", (req, res) => {
    let pDate = req.query;
    console.log(pDate.date);
    leadsCollection.deleteMany({ data_date: pDate.date }).then((result) => {
      res.send(result.deletedCount > 0);
    });
  });
  app.get("/initialLead", (req, res) => {
    let initDate = req.query.initDate;
    console.log(initDate);
    leadsCollection
      .aggregate([
        {
          $match: {
            $and: [
              { for_d: null },
              { Data_Status: "Valid_Data" },
              { data_date: initDate },
            ],
          },
        },
      ])
      .toArray((err, results) => {
        let output = [];
        let users = _.groupBy(
          JSON.parse(JSON.stringify(results)),
          function (d) {
            return d.ba_id;
          }
        );
        for (user in users) {
          output.push({
            // userId: user,
            // consumers: users[user],
            // countByUser: users[user].length,
            initLeads: users[user].slice(0, 10).map((d) => {
              let datas = {};
              (datas.id = d._id),
                (datas.diid = d.diid),
                (datas.Territory = d.Territory),
                (datas.data_date = d.data_date),
                (datas.r_name = d.r_name),
                (datas.Consumer_No = d.Consumer_No);
              return datas;
            }),
          });
        }
        res.send(output);
      });
  });
  app.patch("/updateInitialLead", async (req, res) => {
    const initialLead = req.body;
    console.log(initialLead);
    let buldOperation = [];
    let counter = 0;

    try {
      initialLead.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element.id) },
            update: {
              $set: {
                for_d: element.for_d,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await leadsCollection.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await leadsCollection.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: true,
      });
    } catch (error) {
      console.log(error);
    }
  });
  app.get("/regenerate", (req, res) => {
    const regenDate = req.query.regenDate;
    console.log(regenDate);
    leadsCollection
      .aggregate([
        {
          $match: {
            $and: [{ Data_Status: "Valid_Data" }, { data_date: regenDate }],
          },
        },
      ])
      .toArray((err, results) => {
        let output = [];
        let users = _.groupBy(
          JSON.parse(JSON.stringify(results)),
          function (d) {
            return d.ba_id;
          }
        );
        for (user in users) {
          output.push({
            // userId: user,
            // consumers: users[user],
            // countByUser: users[user].length,
            // callDone: users[user].filter(
            //   (x) => x.answer10 === "yes" || x.answer10 === "no"
            // ).length,
            newLead: users[user]
              .filter(
                (x) =>
                  x.for_d === null &&
                  (x.answer1 === null || x.answer1 === undefined)
              )
              .slice(
                0,
                users[user].filter((x) => x.answer4 === "yes").length < 10
                  ? 11 - users[user].filter((x) => x.answer4 === "yes").length
                  : 0
              )
              .map((d) => {
                let datas = {};
                (datas.id = d._id),
                  (datas.diid = d.diid),
                  (datas.Territory = d.Territory),
                  (datas.data_date = d.data_date),
                  (datas.r_name = d.r_name),
                  (datas.Consumer_No = d.Consumer_No);
                return datas;
              }),
          });
        }
        res.send(output);
      });
  });
  app.patch("/regenerateUpdate", async (req, res) => {
    const regenerateLead = req.body;
    console.log(regenerateLead);
    let buldOperation = [];
    let counter = 0;

    try {
      regenerateLead.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element.id) },
            update: {
              $set: {
                for_d: element.for_d,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await leadsCollection.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await leadsCollection.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: true,
      });
    } catch (error) {
      console.log(error);
    }
  });

  app.post("/setstatus", (req, res) => {
    const status = req.body;
    console.log(status);
    statusCollection.deleteMany({});
    statusCollection.insertOne(status).then((result) => {
      res.send(result.insertedCount > 0);
    });
  });
  app.get("/getStatus", (req, res) => {
    statusCollection.find({}).toArray((err, status) => {
      res.send(status);
    });
  });

  app.get("*", (req, res) => {
    res.sendFile(
      path.join(__dirname, "../bandhan_retailer_client/build", "index.html")
    );
  });
});

app.get("/", (req, res) => {
  res.send("Hello World!");
});

app.listen(process.env.PORT || port);
