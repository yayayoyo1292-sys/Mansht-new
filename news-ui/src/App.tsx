import { useState } from "react";

const BASE_URL = import.meta.env.VITE_API_URL;
fetch(`${BASE_URL}/news/review?reviewer=1`)
type Article = {
    id: number;
    title: string;
    predicted: string;
    confidence: number;
    message?: string;
};

export default function App() {

    const [reviewer, setReviewer] = useState("");
    const [article, setArticle] = useState<Article | null>(null);

    const [loading, setLoading] = useState(false);
    const [started, setStarted] = useState(false);

    // =========================
    // LOAD ARTICLE
    // =========================

    async function loadArticle() {

        try {

            setLoading(true);

            const res = await fetch(
                `${BASE_URL}/news/review?reviewer=${reviewer}`
            );

            const data = await res.json();

            if (data?.message) {
                setArticle(null);
            } else {
                setArticle(data);
            }

        } catch (err) {
            console.error(err);
        } finally {
            setLoading(false);
        }
    }

    // =========================
    // START
    // =========================

    async function startReview() {

        if (!reviewer.trim()) return;

        setStarted(true);
        await loadArticle();
    }

    // =========================
    // SUBMIT
    // =========================

    async function submit(category: string) {

        if (!article) return;

        try {

            setLoading(true);

            await fetch(`${BASE_URL}/news/review`, {

                method: "POST",

                headers: {
                    "Content-Type": "application/json"
                },

                body: JSON.stringify({
                    id: article.id,
                    category,
                    reviewer
                })
            });

            await loadArticle();

        } catch (err) {
            console.error(err);
        } finally {
            setLoading(false);
        }
    }

    // =========================
    // START SCREEN
    // =========================

    if (!started) {

        return (
            <div style={{ padding: 40 }}>

                <h2>Enter your name</h2>

                <input
                    placeholder="Reviewer name"
                    value={reviewer}
                    onChange={(e) => setReviewer(e.target.value)}
                />

                <button onClick={startReview}>
                    Start
                </button>

            </div>
        );
    }

    // =========================
    // LOADING
    // =========================

    if (loading) {
        return <h2>Loading...</h2>;
    }

    // =========================
    // EMPTY STATE (FIXED LOGIC)
    // =========================

    if (started && !article) {
        return <h2>No articles left 🎉</h2>;
    }

    // =========================
    // MAIN UI
    // =========================

    return (

        <div style={{ padding: 40 }}>

            <h2>{article?.title}</h2>

            <p>
                Predicted: {article?.predicted}
                ({article?.confidence})
            </p>

            <div style={{ display: "flex", gap: 10 }}>

                <button onClick={() => submit("رياضة")}>
                    رياضة
                </button>

                <button onClick={() => submit("سياسة")}>
                    سياسة
                </button>

                <button onClick={() => submit("فن")}>
                    فن
                </button>

                <button onClick={() => submit("اجتماعية")}>
                    اجتماعية
                </button>

            </div>

        </div>
    );
}
