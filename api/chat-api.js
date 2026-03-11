import express from "express"
import { generateResponse } from "../chat/chat-engine.js"

const router = express.Router()

router.post("/chat", async (req,res)=>{

 const message = req.body.message

 const reply = await generateResponse(message)

 res.json({ reply })

})

export default router
