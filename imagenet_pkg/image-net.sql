--
-- PostgreSQL database dump
--

-- Dumped from database version 13.1 (Debian 13.1-1.pgdg100+1)
-- Dumped by pg_dump version 13.1 (Debian 13.1-1.pgdg100+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: image-net; Type: DATABASE; Schema: -; Owner: postgres
--

CREATE DATABASE "image-net" WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE = 'en_US.utf8';


ALTER DATABASE "image-net" OWNER TO postgres;

\connect -reuse-previous=on "dbname='image-net'"

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: classes; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.classes (
    wnid character varying(20) NOT NULL,
    words character varying(500),
    hierarchy character varying(2000)
);


ALTER TABLE public.classes OWNER TO postgres;

--
-- Name: ref_url_states; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ref_url_states (
    id integer NOT NULL,
    name character varying(50)
);


ALTER TABLE public.ref_url_states OWNER TO postgres;

--
-- Name: ref_url_states_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.ref_url_states_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ref_url_states_id_seq OWNER TO postgres;

--
-- Name: ref_url_states_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.ref_url_states_id_seq OWNED BY public.ref_url_states.id;


--
-- Name: structure; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.structure (
    release character varying(30) NOT NULL,
    parent_wnid character varying(20) NOT NULL,
    child_wnid character varying(20) NOT NULL
);


ALTER TABLE public.structure OWNER TO postgres;

--
-- Name: url_states; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.url_states (
    url_id integer NOT NULL,
    state_id integer NOT NULL
);


ALTER TABLE public.url_states OWNER TO postgres;

--
-- Name: urls; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.urls (
    id integer NOT NULL,
    release character varying(30) NOT NULL,
    wnid character varying(20) NOT NULL,
    url character varying(2000) NOT NULL
);


ALTER TABLE public.urls OWNER TO postgres;

--
-- Name: urls_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.urls_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.urls_id_seq OWNER TO postgres;

--
-- Name: urls_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.urls_id_seq OWNED BY public.urls.id;


--
-- Name: ref_url_states id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ref_url_states ALTER COLUMN id SET DEFAULT nextval('public.ref_url_states_id_seq'::regclass);


--
-- Name: urls id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.urls ALTER COLUMN id SET DEFAULT nextval('public.urls_id_seq'::regclass);


--
-- Name: classes classes_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.classes
    ADD CONSTRAINT classes_pkey PRIMARY KEY (wnid);


--
-- Name: ref_url_states ref_url_states_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ref_url_states
    ADD CONSTRAINT ref_url_states_pkey PRIMARY KEY (id);


--
-- Name: structure structure_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.structure
    ADD CONSTRAINT structure_pkey PRIMARY KEY (release, parent_wnid, child_wnid);


--
-- Name: url_states url_states_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.url_states
    ADD CONSTRAINT url_states_pkey PRIMARY KEY (url_id);


--
-- Name: urls url_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.urls
    ADD CONSTRAINT url_unique UNIQUE (release, wnid, url);


--
-- Name: urls urls_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.urls
    ADD CONSTRAINT urls_pkey PRIMARY KEY (id);


--
-- Name: urls_index; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX urls_index ON public.urls USING btree (release, wnid);


--
-- Name: structure structure_child_wnid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.structure
    ADD CONSTRAINT structure_child_wnid_fkey FOREIGN KEY (child_wnid) REFERENCES public.classes(wnid);


--
-- Name: structure structure_parent_wnid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.structure
    ADD CONSTRAINT structure_parent_wnid_fkey FOREIGN KEY (parent_wnid) REFERENCES public.classes(wnid);


--
-- Name: url_states url_states_state_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.url_states
    ADD CONSTRAINT url_states_state_id_fkey FOREIGN KEY (state_id) REFERENCES public.ref_url_states(id);


--
-- Name: url_states url_states_url_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.url_states
    ADD CONSTRAINT url_states_url_id_fkey FOREIGN KEY (url_id) REFERENCES public.urls(id);


--
-- Name: urls urls_wnid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.urls
    ADD CONSTRAINT urls_wnid_fkey FOREIGN KEY (wnid) REFERENCES public.classes(wnid);


--
-- PostgreSQL database dump complete
--

