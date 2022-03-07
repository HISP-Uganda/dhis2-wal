import axios from "axios";

export const api = axios.create({
  baseURL: "http://localhost:3001/",
  // baseURL: "https://services.dhis2.hispuganda.org/",
});


