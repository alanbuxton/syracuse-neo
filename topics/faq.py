FAQ = {

    "Who are you and what is this?": "I'm Alan Buxton and this is my open data project.",

    "What sort of open data?": "Syracuse takes raw data from various text documents (currently news articles, but it could be any text document) and uses machine learning to structure the relevant content into linkages and timelines.",

    "Why Syracuse?": "I called my first natural language processing application <a href=\"https://alanbuxton.wordpress.com/2021/09/21/transformers-for-use-oriented-entity-extraction/\">Napoli</a> because it has all the consonants in NLP (the abbreviation for natural language processing). Since then I've given my other projects similarly-themed names related to Ancient Greek Mediterranean coastal towns and cities",

    "How does this offering differ from <provide any other news aggregator>": "The key difference is that this is highly automated with machine learning together with some rules and heuristics. Historically, the tech for doing this sort of thing suffered from what we called the <a href=\"https://alanbuxton.wordpress.com/2023/01/02/revisiting-entity-extraction/\">Bloomberg problem</a>. Briefly, there is plenty of tech available that will tell you what company names a document contains, but figuring out what these companies are doing with each other is a lot harder to do automatically. The companies out there who are doing this sort of thing rely heavily on human analysts to work around the Bloomberg problem. Doing this via machine, which 1145 does, makes it viable to offer it for free as open data",

    "What's 1145?": "This was a domain I bought a long time ago for a company idea that never got off the ground. A lot of programmers collect domain names for side projects and <a href=\"https://www.reddit.com/r/ProgrammerHumor/comments/hp5q56/here_i_go_again/\">we rarely, if ever, part with them</a>. I like the idea of technology making your life easier so that you get your day's work done by lunchtime. So I've kept using 1145.am as the domain to host this application and the umbrella term for all the associated components that feed into the Syracuse application which you're browsing right now.",

    "There are other components to this?": "Yep: Heraklion scrapes data, Alexandria classifies it, Massalia is used to label example texts for machine learning and Corinth takes the Massalia data and adds further synthetic data for machine learning training. Neapolis uses machine learning to extract relevant information from the documents and structures them into an RDF format for Syracuse to ingest and display",

    "Is there any generative AI in here?": "One part of the 1145 system (Neapolis) uses a FlanT5 model to help extract some meaning from text, but the bulk of the machine learning is a fine-tuned RoBERTa model used for classification and then one fine-tuned RoBERTa model per type of activity to do the named entity extraction. Very little GenAI, but plenty of large language models and Machine Learning.",

    "What about the New York Times's law-suit against OpenAI - if OpenAI's work turns out to be illegal then doesn't that risk destroying this project?": "I'm not a lawyer but my common-sense view is that 1145 is more similar to a search engine than to a generative AI service. 1145 helps you find relevant data related to a company or region that you're interested in with full provenance back to the original data source. It's not creating new things that may or may not breach someone's intellectual property. All the scraped data in it is scraped responsibly",

    "What's the license?": "I am licensing the data that you access via the website with an open data share-alike license. The same one that OpenCorporates uses. When I was learning these technologies there weren't many resources available to help, so I'm also open sourcing the Syrcause codebase in the hope that it can help others learn from my learnings. And also that others out there can point out problems in my code that I can learn from and improve.",

    "What about if the share-alike license isn't for me": "Drop me a line and we can discuss. I'm more than happy to charge people for API access or bulk data in order to fund the future development of this project. Very similar to the OpenCorporates approach which I admire greatly.",

    "Is it all open source?": "Nope. Just the Syracuse piece is open source. The other parts of the 1145 ecosystem that are running behind the scenes are top secret proprietary intellectual property :)",

    "Does it suffer from hallucinations?": "Not really, because any generative element is run within very tight guardrails. But there could be other errors creeping in that aren't hallucinations, which is why there's a feedback form that you are free to use if you spot anything that looks wrong",

    "What sort of accuracy does it have?": "No ML system is going to be 100% correct all the time. Accuracy is usually measured by looking at False Positives and False Negatives. A False Positive is when the system says that something happened when it didn't happen. A False Negative is not spotting that something happened. In 1145 my intent is to prioritise minimising false positives. This does increase the risk of false negatives. But 1145 is looking at multiple data sources so even if it misses a topic from one source, we should expect to be find it in another one, so I think that's a reasonable position to take in the accuracy balancing act.",

    "What about Cookies?": "This site does some high level tracking with Google Analytics so I can get a sense of who is accessing the site and from where. It uses its own cookies for [a] authenticating your login and [b] internal security features (so-called 'csrf tokens'). If you never login there will never be a cookie that can identify you at all.",

    "Do you have a Privacy Policy?": "The privacy policy is really simple. This site is run by me (see above). I have no way of knowing anything about anonymous people browsing the site. If you login then I will have access to whatever login data you provide plus which companies you are tracking without seeking your prior consent. I won't provide this data to anyone else for any kind of personal gain. If there are legal reasons that mean I have to hand over this data then I won't have much choice. Where I am legally permitted to do so, I will let you know if this happens",

    "How will you seek my consent or let me know?": "If you've given your email address so you can get tracked organization updates then I will use this to notify you of anything that needs your consent. If you have not given your email address then I won't be able to contact you directly, so I'll flag important notices on this website to give you plenty of time to let me know of any concerns.",

    "How frequently asked are these questions?": "Not at all, to be honest. This page is me writing down an imaginary conversation between me and someone who I'm trying to explain this website to.",

    "Website looks a bit rubbish, mate": "Yep, I'm not going to disagree on that front. Front-end web design really is not my bag - just have a look at the quality of the favicon. But I don't really mind it looking like something from 1997: The focus of this website is on the data, not on looking pretty.",

}
