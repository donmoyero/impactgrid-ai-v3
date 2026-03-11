import { adviserPrompt } from "./adviser-prompt.js"
import { openai } from "../config/openai-client.js"

export async function generateResponse(message){

 const completion = await openai.chat.completions.create({
  model:"gpt-4o-mini",
  messages:[
   { role:"system", content: adviserPrompt },
   { role:"user", content: message }
  ]
 })

 return completion.choices[0].message.content
}
