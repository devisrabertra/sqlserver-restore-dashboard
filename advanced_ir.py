# ==================================================
# ADVANCED INFORMATION RETRIEVAL MODULE
# File: advanced_ir.py
# Lokasi: D:\DASHBOARD_PROJECT\advanced_ir.py
# ==================================================
import pandas as pd
import re
from datetime import datetime, timedelta
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from fuzzywuzzy import fuzz
import dateparser
import numpy as np

class BackupIRSystem:
    """
    ADVANCED IR: Sistem retrieval backup dengan ranking relevansi
    dan natural language processing
    """
    def __init__(self):
        self.vectorizer = TfidfVectorizer(stop_words='english', min_df=1)
        self.queries_processed = 0
        
    def parse_natural_language_query(self, query):
        """
        ADVANCED IR: Parse natural language query menggunakan pattern matching
        dan date parsing
        """
        print(f"🔍 ADVANCED IR: Parsing query: '{query}'")
        
        parsed = {
            'database_name': None,
            'time_period': None,
            'backup_type': None,
            'keywords': [],
            'raw_query': query.lower(),
            'is_natural_language': True
        }
        
        # Extract keywords untuk semantic matching
        words = query.lower().split()
        parsed['keywords'] = [word for word in words if len(word) > 2]
        
        # ADVANCED IR: Extract database name menggunakan multiple patterns
        db_patterns = [
            r'(?:database|db|restore|backup)\s+(\w+)',
            r'(\w+)\s+(?:database|db)',
            r'restore\s+(\w+)\s+(?:database|db)?',
            r'backup\s+(\w+)\s+(?:database|db)?'
        ]
        
        for pattern in db_patterns:
            match = re.search(pattern, query.lower())
            if match:
                potential_db = match.group(1)
                # Filter out common words
                if potential_db not in ['the', 'my', 'our', 'last', 'latest', 'newest']:
                    parsed['database_name'] = potential_db
                    print(f"✅ ADVANCED IR: Database terdeteksi: {parsed['database_name']}")
                    break
        
        # ADVANCED IR: Extract backup type
        type_keywords = {
            'full': 'Database',
            'differential': 'Differential', 
            'log': 'Log',
            'complete': 'Database',
            'incremental': 'Differential'
        }
        
        for keyword, backup_type in type_keywords.items():
            if keyword in query.lower():
                parsed['backup_type'] = backup_type
                print(f"✅ ADVANCED IR: Backup type terdeteksi: {parsed['backup_type']}")
                break
        
        # ADVANCED IR: Extract time period dengan multiple approaches
        time_keywords = {
            'hari ini': 0,
            'kemarin': 1, 
            'minggu lalu': 7,
            '2 minggu': 14,
            'bulan ini': 30,
            'bulan lalu': 30,
            'minggu ini': 7,
            'terbaru': 7,
            'latest': 7,
            'newest': 7,
            'recent': 14
        }
        
        for keyword, days in time_keywords.items():
            if keyword in query.lower():
                parsed['time_period'] = days
                print(f"✅ ADVANCED IR: Time period terdeteksi: {days} hari")
                break
        
        # ADVANCED IR: Advanced date parsing menggunakan dateparser
        if not parsed['time_period']:
            try:
                parsed_date = dateparser.parse(query, languages=['id', 'en'])
                if parsed_date:
                    days_diff = (datetime.now() - parsed_date.replace(tzinfo=None)).days
                    if days_diff >= 0:
                        parsed['time_period'] = days_diff
                        print(f"✅ ADVANCED IR: Date parsing berhasil: {days_diff} hari lalu")
            except Exception as e:
                print(f"⚠️ ADVANCED IR: Date parsing error: {e}")
        
        self.queries_processed += 1
        return parsed
    
    def calculate_semantic_similarity(self, backup_record, keywords):
        """
        ADVANCED IR: Hitung semantic similarity menggunakan TF-IDF
        """
        if not keywords:
            return 0.5  # Default score jika tidak ada keywords
            
        try:
            # Buat corpus dari backup record metadata
            corpus = [
                ' '.join(keywords),  # Query keywords
                f"{backup_record['database_name']} {backup_record.get('backup_type', '')} {backup_record.get('backupset_name', '')}"
            ]
            
            # Hitung TF-IDF vectors
            tfidf_matrix = self.vectorizer.fit_transform(corpus)
            
            # Hitung cosine similarity antara query dan backup record
            similarity = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0]
            
            return max(0, similarity)  # Ensure non-negative
        except Exception as e:
            print(f"⚠️ ADVANCED IR: Semantic similarity error: {e}")
            return 0.3  # Fallback score
    
    def calculate_fuzzy_match_score(self, backup_record, parsed_query):
        """
        ADVANCED IR: Fuzzy string matching untuk database names
        """
        if not parsed_query['database_name']:
            return 0.5
            
        db_name = backup_record['database_name'].lower()
        query_db = parsed_query['database_name'].lower()
        
        # Multiple fuzzy matching techniques
        ratio1 = fuzz.ratio(db_name, query_db)
        ratio2 = fuzz.partial_ratio(db_name, query_db)
        ratio3 = fuzz.token_sort_ratio(db_name, query_db)
        
        # Take the best score
        best_ratio = max(ratio1, ratio2, ratio3)
        
        # Normalize to 0-1 scale
        fuzzy_score = best_ratio / 100.0
        
        print(f"🔍 ADVANCED IR: Fuzzy match '{db_name}' vs '{query_db}': {fuzzy_score}")
        return fuzzy_score
    
    def calculate_recency_score(self, backup_record, parsed_query):
        """
        ADVANCED IR: Scoring berdasarkan recency dengan exponential decay
        """
        try:
            backup_date = backup_record['backup_finish_date']
            if isinstance(backup_date, str):
                # Convert string to datetime
                backup_date = datetime.fromisoformat(backup_date.replace('Z', '+00:00'))
            
            days_ago = (datetime.now() - backup_date).days
            
            if parsed_query['time_period'] is not None:
                # User specified time period
                if days_ago <= parsed_query['time_period']:
                    # Exponential decay within specified period
                    recency_score = np.exp(-days_ago / max(parsed_query['time_period'], 1))
                else:
                    recency_score = 0
            else:
                # Default: exponential decay dengan half-life 30 hari
                recency_score = np.exp(-days_ago / 30)
            
            return min(1.0, max(0.0, recency_score))
            
        except Exception as e:
            print(f"⚠️ ADVANCED IR: Recency calculation error: {e}")
            return 0.5
    
    def calculate_completeness_score(self, backup_record):
        """
        ADVANCED IR: Scoring berdasarkan completeness (backup type dan size)
        """
        type_scores = {
            'Database': 1.0,      # Full backup - most complete
            'Differential': 0.7,  # Differential - somewhat complete
            'Log': 0.3           # Log backup - least complete
        }
        
        backup_type = backup_record.get('backup_type', 'Database')
        type_score = type_scores.get(backup_type, 0.5)
        
        # Size-based scoring (normalized)
        size_score = 0.5
        if backup_record.get('backup_size'):
            # Normalize size (asumsi backup size > 100MB lebih baik)
            size_mb = backup_record['backup_size'] / (1024 * 1024)
            size_score = min(1.0, size_mb / 500)  # Normalize by 500MB
        
        return (type_score * 0.7 + size_score * 0.3)
    
    def calculate_relevance_score(self, backup_record, parsed_query):
        """
        ADVANCED IR: Hitung overall relevance score dengan weighted factors
        """
        weights = {
            'fuzzy_match': 0.35,      # Database name matching
            'recency': 0.25,          # How recent
            'semantic': 0.20,         # Semantic similarity
            'completeness': 0.20      # Backup type and size
        }
        
        # Calculate individual scores
        fuzzy_score = self.calculate_fuzzy_match_score(backup_record, parsed_query)
        recency_score = self.calculate_recency_score(backup_record, parsed_query)
        semantic_score = self.calculate_semantic_similarity(backup_record, parsed_query['keywords'])
        completeness_score = self.calculate_completeness_score(backup_record)
        
        # Weighted combination
        total_score = (
            fuzzy_score * weights['fuzzy_match'] +
            recency_score * weights['recency'] +
            semantic_score * weights['semantic'] +
            completeness_score * weights['completeness']
        )
        
        # Debug information
        if total_score > 0.7:  # Only log high-scoring matches
            print(f"🎯 ADVANCED IR: High score {total_score:.3f} for {backup_record['database_name']}")
            print(f"   Fuzzy: {fuzzy_score:.3f}, Recency: {recency_score:.3f}, Semantic: {semantic_score:.3f}, Complete: {completeness_score:.3f}")
        
        return round(total_score, 3)
    
    def rank_backups(self, backup_data, query_text):
        """
        ADVANCED IR: Main function - Rank backups berdasarkan natural language query
        """
        print(f"🚀 ADVANCED IR: Starting ranking for query: '{query_text}'")
        print(f"📊 ADVANCED IR: Processing {len(backup_data)} backup records")
        
        start_time = datetime.now()
        
        # Parse the natural language query
        parsed_query = self.parse_natural_language_query(query_text)
        
        # Calculate scores for each backup
        ranked_results = []
        for backup in backup_data:
            score = self.calculate_relevance_score(backup, parsed_query)
            ranked_results.append({
                **backup,
                'relevance_score': score,
                'matched_criteria': parsed_query,
                'rank_explanation': self.generate_explanation(backup, score, parsed_query)
            })
        
        # Filter out very low scores dan sort by relevance
        ranked_results = [r for r in ranked_results if r['relevance_score'] > 0.1]
        ranked_results.sort(key=lambda x: x['relevance_score'], reverse=True)
        
        processing_time = (datetime.now() - start_time).total_seconds()
        print(f"✅ ADVANCED IR: Ranking completed in {processing_time:.2f}s")
        print(f"📈 ADVANCED IR: Found {len(ranked_results)} relevant backups")
        
        return ranked_results
    
    def generate_explanation(self, backup, score, parsed_query):
        """
        ADVANCED IR: Generate human-readable explanation untuk ranking
        """
        explanations = []
        
        if score > 0.8:
            explanations.append("Sangat relevan dengan permintaan Anda")
        elif score > 0.6:
            explanations.append("Relevan dengan pencarian")
        elif score > 0.4:
            explanations.append("Cukup relevan")
        else:
            explanations.append("Sedikit relevan")
        
        if parsed_query['database_name']:
            explanations.append(f"mencocokkan '{parsed_query['database_name']}'")
        
        if parsed_query['time_period'] is not None:
            explanations.append(f"dalam {parsed_query['time_period']} hari terakhir")
        
        return " • ".join(explanations)

# Global instance
ir_system = BackupIRSystem()