import unittest
from unittest.mock import MagicMock, AsyncMock, patch
import asyncio
from datetime import datetime, timezone

# Set mock env variables before importing main
import os
os.environ["BOT_TOKEN"] = "mock_token"
os.environ["MONGO_URL"] = "mongodb://mock"
os.environ["ADMIN_USER_ID"] = "12345"

import main
from telegram import Update, User, Chat
from telegram.constants import ChatType

class TestBanAllFeatures(unittest.TestCase):

    def setUp(self):
        # Create a mock MongoDBManager
        self.mock_mongo = MagicMock()
        main.mongo_manager = self.mock_mongo

    def test_save_chat_member(self):
        # Test that save_chat_member calls update_one correctly on members_collection
        mock_coll = MagicMock()
        self.mock_mongo.members_collection = mock_coll

        main.MongoDBManager.save_chat_member(self.mock_mongo, chat_id=111, user_id=222, bot_username="testbot")

        mock_coll.update_one.assert_called_once()
        args, kwargs = mock_coll.update_one.call_args
        self.assertEqual(args[0], {'chat_id': 111, 'user_id': 222, 'bot_username': 'testbot'})
        self.assertTrue(kwargs['upsert'])

    def test_get_chat_members(self):
        # Test get_chat_members retrieves user IDs correctly
        mock_coll = MagicMock()
        self.mock_mongo.members_collection = mock_coll
        mock_coll.find.return_value = [
            {'user_id': 101},
            {'user_id': 102}
        ]

        res = main.MongoDBManager.get_chat_members(self.mock_mongo, chat_id=111, bot_username="testbot")
        self.assertEqual(res, [101, 102])
        mock_coll.find.assert_called_once_with({'chat_id': 111, 'bot_username': 'testbot'})

    @patch('main.mongo_manager')
    async def async_test_track_all_updates(self, mock_mongo_inst):
        # Test tracking via update
        update = MagicMock(spec=Update)
        chat = MagicMock(spec=Chat)
        chat.id = 999
        chat.type = ChatType.SUPERGROUP
        user = MagicMock(spec=User)
        user.id = 888

        update.effective_chat = chat
        update.effective_user = user
        update.chat_member = None

        context = MagicMock()
        context.bot.username = "mybot"

        await main.track_all_updates(update, context)
        mock_mongo_inst.save_chat_member.assert_called_once_with(999, 888, "mybot")

    def test_track_all_updates_wrapper(self):
        asyncio.run(self.async_test_track_all_updates())

    @patch('main.mongo_manager')
    async def async_test_banall_command_unauthorized(self, mock_mongo_inst):
        # Test unauthorized access fails silently (returns None, does not reply)
        update = MagicMock(spec=Update)
        chat = MagicMock(spec=Chat)
        chat.id = 999
        chat.type = ChatType.SUPERGROUP
        user = MagicMock(spec=User)
        user.id = 99999 # Some random unauthorized user

        update.effective_chat = chat
        update.effective_user = user
        update.message = AsyncMock()

        context = MagicMock()
        context.bot.username = "mybot"

        # main admin is 12345, clone is None
        mock_mongo_inst.get_clone_by_username.return_value = None

        await main.banall_command(update, context)
        # Should NOT reply since unauthorized user
        update.message.reply_text.assert_not_called()

    def test_banall_command_unauthorized_wrapper(self):
        asyncio.run(self.async_test_banall_command_unauthorized())

    @patch('main.mongo_manager')
    async def async_test_banall_command_authorized_admin(self, mock_mongo_inst):
        # Test main admin can trigger banall
        update = MagicMock(spec=Update)
        chat = MagicMock(spec=Chat)
        chat.id = 999
        chat.type = ChatType.SUPERGROUP
        user = MagicMock(spec=User)
        user.id = 12345 # Main admin user ID

        update.effective_chat = chat
        update.effective_user = user
        update.message = AsyncMock()

        context = MagicMock()
        context.bot.username = "mybot"
        context.bot.id = 777
        context.bot.get_chat_administrators = AsyncMock(return_value=[])
        context.bot.ban_chat_member = AsyncMock(return_value=True)

        mock_mongo_inst.get_clone_by_username.return_value = None
        mock_mongo_inst.get_chat_members.return_value = [101, 102, 12345] # 12345 is self, should be excluded

        await main.banall_command(update, context)

        # Should reply with starting message and completion message
        update.message.reply_text.assert_called_once()
        self.assertEqual(context.bot.ban_chat_member.call_count, 2) # Should ban 101 and 102

    def test_banall_command_authorized_admin_wrapper(self):
        asyncio.run(self.async_test_banall_command_authorized_admin())

    @patch('main.mongo_manager')
    async def async_test_banall_command_authorized_clone_owner(self, mock_mongo_inst):
        # Test clone owner can trigger banall on their cloned bot
        update = MagicMock(spec=Update)
        chat = MagicMock(spec=Chat)
        chat.id = 999
        chat.type = ChatType.SUPERGROUP
        user = MagicMock(spec=User)
        user.id = 55555 # Clone owner ID

        update.effective_chat = chat
        update.effective_user = user
        update.message = AsyncMock()

        context = MagicMock()
        context.bot.username = "clonedbot"
        context.bot.id = 777
        context.bot.get_chat_administrators = AsyncMock(return_value=[])
        context.bot.ban_chat_member = AsyncMock(return_value=True)

        mock_mongo_inst.get_clone_by_username.return_value = {'user_id': 55555, 'bot_username': 'clonedbot'}
        mock_mongo_inst.get_chat_members.return_value = [101, 102, 55555] # 55555 is self, should be excluded

        await main.banall_command(update, context)

        update.message.reply_text.assert_called_once()
        self.assertEqual(context.bot.ban_chat_member.call_count, 2) # Should ban 101 and 102

    def test_banall_command_authorized_clone_owner_wrapper(self):
        asyncio.run(self.async_test_banall_command_authorized_clone_owner())

if __name__ == '__main__':
    unittest.main()
