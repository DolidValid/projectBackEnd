import { Router } from "express";
import { addInfoFiles, active4GHandler,fetchJobsHandler } from "../controllers/userController.js";

/**
 * Express router instance for handling user-related routes.
 * @type {import('express').Router}
 */
const router = Router();

// Add a user
router.post("/add-user", addInfoFiles);

// New active4G API
router.post("/active4G", active4GHandler);

// New search API
router.post("/Search",fetchJobsHandler );

export default router;
