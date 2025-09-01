import { Router } from "express";
import addInfoFiles  from "../controllers/userController.js";

/**
 * Express router instance for handling user-related routes.
 * @type {import('express').Router}
 */
const router = Router();

router.post("/add-user", addInfoFiles);

export default router;


