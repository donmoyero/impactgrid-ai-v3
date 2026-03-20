// ================================================================
//  ImpactGrid — Supabase Clients
//  config/supabase-client.js
//
//  Two projects:
//    supabase      — Creator Intelligence (trends, videos, ingestion)
//    groupSupabase — ImpactGrid Group (analytics, financial)
// ================================================================

import { createClient } from "@supabase/supabase-js";

export const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY || process.env.SUPABASE_KEY
);
