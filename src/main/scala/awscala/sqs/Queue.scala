package awscala.sqs

case class Queue(url: String, sqs: SQS) {

  def messages(): Seq[Message] = sqs.receiveMessage(this)
  def messageBatch(batchSize: Int): Seq[Message] = sqs.receiveMessageBatch(this, batchSize)

  def add(messages: String*) = sqs.sendMessages(this, messages)
  def addAll(messages: Seq[String]) = sqs.sendMessages(this, messages)

  def remove(messages: Message*) = sqs.deleteMessages(messages)
  def removeAll(messages: Seq[Message]) = sqs.deleteMessages(messages)

  def destroy() = sqs.deleteQueue(this)

}
