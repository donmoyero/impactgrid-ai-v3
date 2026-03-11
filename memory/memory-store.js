import { supabase } from "../config/supabase-client.js"

export async function saveUser(name,business){

 await supabase
  .from("users")
  .insert([
   { name:name, business_name:business }
  ])
}
