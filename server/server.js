import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import Groq from "groq-sdk";

dotenv.config();

const app  = express();
const groq = new Groq({ apiKey: process.env.GROQ_API_KEY });

app.use(cors());
app.use(express.json());

/* ── Health check ── */
app.get("/health", (req, res) => {
  res.json({ status: "ok", service: "ImpactGrid Dijo", ts: new Date().toISOString() });
});

/* ── Chat endpoint ── */
app.post("/chat", async (req, res) => {
  try {
    const { message, mode = "adviser" } = req.body;
    if (!message) return res.status(400).json({ error: "No message provided" });

    const systemPrompts = {
      adviser: `You are Dijo, ImpactGrid's AI financial adviser for small business owners. 
You speak plainly and directly — no jargon, no waffle. 
You give specific, actionable financial advice based on the data provided.
Always be honest even if the news is tough. Keep responses concise and practical.
Format responses clearly with short paragraphs. Use bullet points sparingly.`,

      dashboard: `You are Dijo, an AI financial analyst embedded in ImpactGrid.
Analyse the financial data provided and give clear, specific insights.
Focus on trends, risks, and opportunities. Be direct and numbers-focused.`,

      group: `You are Dijo, ImpactGrid's AI adviser for the ImpactGrid Group platform.
You help business owners and investors understand financial performance.
Be professional, clear, and actionable.`
    };

    const systemPrompt = systemPrompts[mode] || systemPrompts.adviser;

    const completion = await groq.chat.completions.create({
      model:       "llama-3.3-70b-versatile",
      max_tokens:  1024,
      temperature: 0.7,
      messages: [
        { role: "system",  content: systemPrompt },
        { role: "user",    content: message }
      ]
    });

    const reply = completion.choices[0]?.message?.content || "I couldn't generate a response.";
    res.json({ reply });

  } catch (err) {
    console.error("[Dijo] Error:", err.message);
    res.status(500).json({ error: "AI service error", details: err.message });
  }
});

/* ── Keep-warm ping (called by frontend) ── */
app.get("/ping", (req, res) => res.json({ pong: true }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`ImpactGrid Dijo running on port ${PORT}`);
});
