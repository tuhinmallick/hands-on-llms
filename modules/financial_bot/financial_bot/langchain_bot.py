import logging
from pathlib import Path
from typing import Iterable, List, Tuple

from langchain import chains
from langchain.memory import ConversationBufferWindowMemory

from financial_bot import constants
from financial_bot.chains import (
    ContextExtractorChain,
    FinancialBotQAChain,
    StatelessMemorySequentialChain,
)
from financial_bot.embeddings import EmbeddingModelSingleton
from financial_bot.models import build_huggingface_pipeline
from financial_bot.qdrant import build_qdrant_client
from financial_bot.template import get_llm_template

logger = logging.getLogger(__name__)


class FinancialBot:
    def __init__(
        self,
        llm_model_id: str = constants.LLM_MODEL_ID,
        llm_lora_model_id: str = constants.LLM_QLORA_CHECKPOINT,
        llm_template_name: str = constants.TEMPLATE_NAME,
        vector_collection_name: str = constants.VECTOR_DB_OUTPUT_COLLECTION_NAME,
        vector_db_search_topk: int = constants.VECTOR_DB_SEARCH_TOPK,
        model_cache_dir: Path = constants.CACHE_DIR,
        streaming: bool = False,
        embedding_model_device: str = "cuda:0",
        debug: bool = False,
    ):
        self._llm_template_name = llm_template_name
        self._llm_template = get_llm_template(name=self._llm_template_name)

        self._vector_collection_name = vector_collection_name
        self._vector_db_search_topk = vector_db_search_topk
        self._qdrant_client = build_qdrant_client()

        self._embd_model = EmbeddingModelSingleton(
            cache_dir=model_cache_dir, device=embedding_model_device
        )
        self._llm_agent, self._streamer = build_huggingface_pipeline(
            llm_model_id=llm_model_id,
            llm_lora_model_id=llm_lora_model_id,
            use_streamer=streaming,
            cache_dir=model_cache_dir,
            debug=debug,
        )
        self.finbot_chain = self.build_chain()

    @property
    def is_streaming(self) -> bool:
        return self._streamer is not None

    def build_chain(self) -> chains.SequentialChain:
        """
        Constructs and returns a financial bot chain.
        This chain is designed to take as input the user description, `about_me` and a `question` and it will
        connect to the VectorDB, searches the financial news that rely on the user's question and injects them into the
        payload that is further passed as a prompt to a financial fine-tuned LLM that will provide answers.

        The chain consists of two primary stages:
        1. Context Extractor: This stage is responsible for embedding the user's question,
        which means converting the textual question into a numerical representation.
        This embedded question is then used to retrieve relevant context from the VectorDB.
        The output of this chain will be a dict payload.

        2. LLM Generator: Once the context is extracted,
        this stage uses it to format a full prompt for the LLM and
        then feed it to the model to get a response that is relevant to the user's question.

        Returns
        -------
        chains.SequentialChain
            The constructed financial bot chain.

        Notes
        -----
        The actual processing flow within the chain can be visualized as:
        [about: str][question: str] > ContextChain >
        [about: str][question:str] + [context: str] > FinancialChain >
        [answer: str]
        """

        logger.info("Building 1/3 - ContextExtractorChain")
        context_retrieval_chain = ContextExtractorChain(
            embedding_model=self._embd_model,
            vector_store=self._qdrant_client,
            vector_collection=self._vector_collection_name,
            top_k=self._vector_db_search_topk,
        )

        logger.info("Building 2/3 - FinancialBotQAChain")
        llm_generator_chain = FinancialBotQAChain(
            hf_pipeline=self._llm_agent,
            template=self._llm_template,
        )

        logger.info("Building 3/3 - Connecting chains into SequentialChain")
        seq_chain = StatelessMemorySequentialChain(
            history_input_key="to_load_history",
            memory=ConversationBufferWindowMemory(
                memory_key="chat_history",
                input_key="question",
                output_key="answer",
                k=3,
            ),
            chains=[context_retrieval_chain, llm_generator_chain],
            input_variables=["about_me", "question", "to_load_history"],
            output_variables=["answer"],
            verbose=True,
        )

        logger.info("Done building SequentialChain.")
        logger.info("Workflow:")
        logger.info(
            """
            [about: str][question: str] > ContextChain > 
            [about: str][question:str] + [context: str] > FinancialChain > 
            [answer: str]
            """
        )

        return seq_chain

    def answer(
        self,
        about_me: str,
        question: str,
        to_load_history: List[Tuple[str, str]] = None,
    ) -> str:
        """
        Given a short description about the user and a question make the LLM
        generate a response.

        Parameters
        ----------
        about_me : str
            Short user description.
        question : str
            User question.

        Returns
        -------
        str
            LLM generated response.
        """

        inputs = {
            "about_me": about_me,
            "question": question,
            "to_load_history": to_load_history if to_load_history else [],
        }
        return self.finbot_chain.run(inputs)

    def stream_answer(self) -> Iterable[str]:
        """Stream the answer from the LLM after each token is generated after calling `answer()`."""

        assert (
            self.is_streaming
        ), "Stream answer not available. Build the bot with `use_streamer=True`."

        partial_answer = ""
        for new_token in self._streamer:
            if new_token != self._llm_template.eos:
                partial_answer += new_token

                yield partial_answer
