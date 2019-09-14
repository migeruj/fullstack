var aws = require('aws-sdk');
const mongoose = require('mongoose');
const uri = 'mongodb://ec2-instance:27017/collection';
const moment = require('moment-timezone');

let conn = null;
var cdate = moment().tz('America/Bogota');

const snsSchema = new mongoose.Schema({
    _id: {type: mongoose.Types.ObjectId},
    messageid: {type: mongoose.Schema.Types.String, required:true, unique: true},
    bounce: {
        type: mongoose.Schema.Types.Mixed,
        default: {},
    },
    complaint: {
        type: mongoose.Schema.Types.Mixed,
        default: {},
    },
    delivery: {
        type: mongoose.Schema.Types.Mixed,
        default: {},
    },
    send: {
        type: mongoose.Schema.Types.Mixed,
        default: {},
    },
    reject: {
        type: mongoose.Schema.Types.Mixed,
        default: {},
    },
    open: {
        type: [Object],
        default: [],
    },
    click: {
        type: [Object],
        default: [],
    },
    failure: {
        type: mongoose.Schema.Types.Mixed,
        default: {},
    },
}, {
    strict: false
  });

const notification = mongoose.model('sns_tracking', snsSchema);


exports.handler = function(event, context, callback)
{
  var SESMessage = event.Records[0].Sns.Message
  SESMessage = JSON.parse(SESMessage);
  var SESMessageType = event.Records[0].Sns.Type;
  var SESMessageId = SESMessage.mail.messageId;
  var SESeventType = SESMessage.eventType;
  if (conn == null) {
    mongoose.connect(uri, {
         "auth": { "authSource": "admin" },
         "user": "root",
         "pass": "password",
         "useNewUrlParser": true
     });
    conn = mongoose.connection;
    conn.on('error', function() {
        console.log("Error");
    });
    conn.once('open', function() {
     // Ohhh yes!
        console.log("we're connected!");
    });
}
function upsertcase(content){
    notification.findOneAndUpdate({messageid: SESMessageId},content,{upsert: true, new: true, setDefaultsOnInsert: true},function(error, result) {
        if(error){
            console.log("Un error ha ocurrido en la query: ", error);
        }else{
            console.log("Un error ha ocurrido en la query: ", result);
        }
    });
}


if (SESMessageType == 'Notification'){
    var ses_not = new notification({messageid: SESMessageId});
    const q = notification.findOne({messageid: SESMessageId});
    q.exec(function(err, results) {
        if(!err) {
            if(results !=undefined){
            switcher(SESeventType,true,results);
            }else{
            switcher(SESeventType,false,results);
            }
        } else {
            console.log(err);
        }
    });
    function switcher(SESeventType,exist,result=null){
        switch(SESeventType){
            case 'Bounce':
                ses_not.bounce=SESMessage.bounce;
                upsertcase(ses_not);
                break;
            case 'Complaint':
                ses_not.complaint=SESMessage.complaint; //Mostar solo al usuario complaintFeedbackType. Es necesario guardar el complaint para resolver los casos de reclamación para la revisión trimestral
                upsertcase(ses_not);
                break;
            case 'Delivery':
                ses_not.delivery={
                    'time': SESMessage.delivery.timestamp,
                    'response': SESMessage.delivery.smtpResponse
                };
                upsertcase(ses_not);
                break;
            case 'Send':
                ses_not.send = {
                    'time': SESMessage.mail.timestamp,
                    'response': 'Enviado'
                };
                upsertcase(ses_not);
                break;                    
            case 'Reject':
                ses_not.reject = {
                    "reason": SESMessage.reject.reason
                };
                upsertcase(ses_not);
                break;
            case 'Open':
                var temp = result;
                var array = temp.open
                if(array.length >=1 && exist){
                    array.push(SESMessage.open);
                    temp.open = array;
                    temp.save();
                }
                else{
                    ses_not.open = [SESMessage.open];
                    upsertcase(ses_not);
                }
                break;
            case 'Click':
                var temp = result;
                var array = temp.click;
                if(array.length >=1 && exist){
                    array.push(SESMessage.click);
                    temp.click = array;
                    temp.save();
                }
                else{
                    ses_not.click = [SESMessage.click];
                    upsertcase(ses_not);
                }
                break;
            case 'Rendering Failure':
                ses_not.failure = SESMessage.failure;
                upsertcase(ses_not);
                break;
        }
    }
}
};