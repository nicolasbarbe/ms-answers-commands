package main

import ( 
        "github.com/julienschmidt/httprouter"
        "github.com/unrolled/render"
        "gopkg.in/mgo.v2"
        "github.com/nicolasbarbe/kafka"
        "encoding/json" 
        "net/http"
        "strconv"
        "time"
        "fmt"
        "log"
)


/** Types **/

type NormalizedAnswer struct {
  Id            string             `json:"id"            bson:"_id"`
  Content       string             `json:"content"       bson:"content"`
  Author        string             `json:"author"        bson:"author"`
  CreatedAt     time.Time          `json:"createdAt"     bson:"createdAt"`
  Discussion    string             `json:"discussion"    bson:"discussion"`
}

// Discussion representats a started discussion
type NormalizedDiscussion struct {
  Id            string             `json:"id"            bson:"_id"`
  Title         string             `json:"title"         bson:"title"`
  Description   string             `json:"description"   bson:"description"`
  Initiator     string             `json:"initiator"     bson:"initiator"`
  CreatedAt     time.Time          `json:"createdAt"     bson:"createdAt"`
}


type NormalizedUser struct {
  Id            string             `json:"id"            bson:"_id"`
  FirstName     string             `json:"firstName"     bson:"firstName"`
  LastName      string             `json:"lastName"      bson:"lastName"`
  MemberSince   time.Time          `json:"memberSince"   bson:"memberSince"`
}


// Controller embeds the logic of the microservice
type Controller struct {
  mongo         *mgo.Database
  producer      *kafka.Producer
  renderer      *render.Render
}

func (this *Controller) PostAnswer(response http.ResponseWriter, request *http.Request, params httprouter.Params ) {

  // build answer from the request
  answer := new(NormalizedAnswer)
  err := json.NewDecoder(request.Body).Decode(answer)
  if err != nil {
    this.renderer.JSON(response, 422, "Request body is not a valid JSON")
    return
  }

   // check that discussion exists
  count, err := this.mongo.C(discussionsCollection).FindId(answer.Discussion).Limit(1).Count()
  if err != nil {
      this.renderer.JSON(response, 500, err)
      return
  }
  if count <= 0 {
    this.renderer.JSON(response, 422, "Discussion does not exist")
    return
  }  
  
  // check that author exists
  count, err = this.mongo.C(usersCollection).FindId(answer.Author).Limit(1).Count()
  if err != nil {
      this.renderer.JSON(response, 500, err)
      return
  }
  if count <= 0 {
    this.renderer.JSON(response, 422, "Author of the answer does not exist")
    return
  }  
  
  // persits answer
  if err := this.mongo.C(answersCollection).Insert(answer) ; err != nil {
    log.Printf("Cannot create document in collection %s : %s", answersCollection, err)
    this.renderer.JSON(response, 500, "Cannot save answer")
    return
  }

  // create message
  body, err := json.Marshal(answer)
  if err != nil {
    log.Printf("Cannot marshall document : %s", err)
    this.renderer.JSON(response, 500, "Cannot process the request")
    return
  }

  message := append([]byte(fmt.Sprintf("%02d%v",len(answerPosted), answerPosted)), body ...)

  // send message
  err = this.producer.SendMessageToTopic(message, answersTopic)
  if err != nil {
    this.renderer.JSON(response, http.StatusInternalServerError, "Failed to send the message: " + err.Error())
    return
  } 

  // render the response
  this.renderer.JSON(response, 200, "ok")
}


func (this *Controller) ConsumeUsers(message []byte) {
  idx, _    := strconv.Atoi(string(message[:2]))
  eventType := string(message[2:idx+2])
  body      := message[idx+2:]

  if eventType != userCreated {
    log.Printf("Message with type %v is ignored. Type %v was expected", eventType, userCreated)
    return
  }

  // unmarshal user from event body
  var normalizedUser NormalizedUser
  if err := json.Unmarshal(body, &normalizedUser); err != nil {
    log.Print("Cannot unmarshal user")
    return
  }

  // save user Id
  if err := this.mongo.C(usersCollection).Insert(normalizedUser) ; err != nil {
    log.Printf("Cannot save document in collection %s : %s", usersCollection, err)
    return
  }
  // todo : We should save only the id, not the whole object. 
}

func (this *Controller) ConsumeDiscussions(message []byte) {
  idx, _    := strconv.Atoi(string(message[:2]))
  eventType := string(message[2:idx+2])
  body      := message[idx+2:]

  if eventType != discussionStarted {
    log.Printf("Message with type %v is ignored. Type %v was expected", eventType, discussionStarted)
    return
  }

  // unmarshal discussion from event body
  var normalizedDiscussion NormalizedDiscussion
  if err := json.Unmarshal(body, &normalizedDiscussion); err != nil {
    log.Print("Cannot unmarshal discussion")
    return
  }

  // save discussion Id
  if err := this.mongo.C(discussionsCollection).Insert(normalizedDiscussion) ; err != nil {
    log.Printf("Cannot save document in collection %s : %s", discussionsCollection, err)
    return
  }
  // todo : We should save only the id, not the whole object. 
}



