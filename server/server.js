import express from "express"
import cors from "cors"
import dotenv from "dotenv"

import chatAPI from "../api/chat-api.js"

dotenv.config()

const app = express()

app.use(cors())
app.use(express.json())

app.use("/api", chatAPI)

const PORT = process.env.PORT || 3000

app.listen(PORT,()=>{
 console.log("ImpactGrid AI v3 running on port "+PORT)
})
